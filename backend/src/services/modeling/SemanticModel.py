import json
import re
import duckdb
from utils.db.lancedb import LanceConnectionFactory
from groq import Groq, RateLimitError, BadRequestError
import pyarrow as pa
from os import getenv as env
from services.modeling.prompts import SEMANTIC_MODEL_PROMPR
from utils.duckdb_util import DuckdbUtil


DEFAULT_RULES = [
    (r'.*_id$|^id$', 'identifier', 0.95),
    (r'.*_at$|.*_date$|^date.*|.*date$', 'date', 0.95),
    (r'.*email.*', 'email', 0.98),
    (r'.*phone.*|.*mobile.*', 'phone_number', 0.95),
    (r'.*customer.*|.*client.*|.*cust.*', 'customer_identifier', 0.92),
    (r'.*name.*', 'name', 0.90),
    (r'.*amount.*|.*price.*|.*salary.*|.*revenue.*|.*total.*', 'financial_value', 0.90),
    (r'.*address.*|.*street.*|.*city.*|.*zip.*', 'address', 0.88),
    (r'.*status.*|.*state.*', 'status', 0.85),
    (r'.*count.*|.*qty.*|.*quantity.*', 'metric', 0.85),
]

SEMANTIC_ONLY_FIELDS = {
    "semantic_concept": "cast(null as string)",
    "confidence_score": "cast(null as float)",
    "description": "cast(null as string)",
    "source": "cast(null as string)",
    "validated": "cast(0 as int)",
    "validated_by": "cast(null as string)",
    "validated_at": "cast(null as int)",
    "is_deleted": "cast(null as int)",
}


EMBEDDING_MODEL = "intfloat/multilingual-e5-small"
EMBEDDING_DIMS = 384

class SemanticModel:
    """Generates and stores semantic concepts for pipeline columns.
    Writes via LanceDB (MVCC), reads via DuckDB SQL.
    Rule-based first, Groq LLM fallback for unmatched columns.
    """

    _embedding_model = None

    @staticmethod
    def _get_lance_path(dbs_path=None) -> str:
        return LanceConnectionFactory.get(dbs_path)


    @staticmethod
    def _get_table(dbs_path=None):
        """Opens or creates the LanceDB column_catalog table. Safe for concurrent first-run."""
        db = LanceConnectionFactory.get(dbs_path)
        try:
            return db.open_table('column_catalog')
        except Exception:
            ...


    @staticmethod
    def _get_duckdb_conn() -> duckdb.DuckDBPyConnection:
        """Returns a DuckDB connection with a column_catalog view over LanceDB files."""
        lance_path = DuckdbUtil.workspacedb_path+'/catalog.lance'
        con = duckdb.connect()
        con.execute("LOAD lance")
        con.execute(f"CREATE VIEW column_catalog AS SELECT * FROM '{lance_path}/column_catalog.lance'")
        return con


    @staticmethod
    def _match_rules(columns: list[dict], rules=None) -> tuple[list[dict], list[dict]]:
        """Splits columns into rule-matched and unmatched.
            Returns:
                matched — list of dicts with semantic_concept, confidence_score, source='rule'
                unmatched — list of original column dicts to send to LLM
        """
        rules = rules or DEFAULT_RULES
        matched, unmatched = [], []

        for col in columns:
            col_name, hit = col['original_column_name'].lower(), None
            
            for pattern, concept, confidence in rules:
                if re.search(pattern, col_name, re.IGNORECASE):
                    hit = (concept, confidence)
                    break
            if hit:
                matched.append({
                    **col, 'semantic_concept':  hit[0], 'confidence_score':  hit[1], 
                    'description': '', 'source': 'rule',
                })
            else:
                unmatched.append(col)

        return matched, unmatched


    @staticmethod
    def get_data_catalog_semantic_model(columns: list[dict]) -> list[dict]:
        """Calls Groq to classify unmatched columns.
        Returns list of dicts with semantic_concept, confidence_score, description.
        """
        if not columns: return []

        col_lines = '\n'.join(
            f'- {c["original_column_name"]} ({c["data_type"]}) from table {c["table_name"]}'
            for c in columns
        )

        try:
            api_key = env('GROQ_API_KEY')
            client = Groq(api_key=api_key)
            response = client.chat.completions.create(
                model='openai/gpt-oss-120b', messages=[{'role': 'user', 'content': SEMANTIC_MODEL_PROMPR.format(col_lines)}],
                stream=False, temperature=0.3, max_tokens=4096,
            )

            raw = response.choices[0].message.content.strip()
            raw = re.sub(r'^```(?:json)?|```$', '', raw, flags=re.MULTILINE).strip()
            llm_results = json.loads(raw)

            result_map = {f"{r['table_name']}_{r['original_column_name']}": r for r in llm_results}
            enriched = []
            for col in columns:
                key = f"{col['table_name']}_{col['original_column_name']}"
                r = result_map.get(key, {})
                enriched.append({
                    **col, 'source': 'llm',
                    'semantic_concept': r.get('semantic_concept', 'unknown'),
                    'confidence_score': float(r.get('confidence_score', 0.5)),
                    'description': r.get('description', ''),
                })

            return enriched

        except (RateLimitError, BadRequestError, Exception) as e:
            print(f'Groq call failed: {str(e)}')
            return []
    

    @staticmethod
    def get_semantic_model(
        rows_to_insert: list,
        new_columns:   list[dict],
        dbs_path:      str  = None,
    ) -> int:

        if not new_columns: return 0

        matched, unmatched = SemanticModel._match_rules(new_columns, DEFAULT_RULES)

        llm_results = []
        if unmatched:
            llm_results = SemanticModel.get_data_catalog_semantic_model(unmatched)

        semantic_map = {
            f"{r['table_name']}_{r['original_column_name']}": r
            for r in matched + llm_results
        }

        for row in rows_to_insert:
            key = f"{row['table_name']}_{row['original_column_name']}"
            sem = semantic_map.get(key, {})
            row['semantic_concept'] = sem.get('semantic_concept', '')
            row['confidence_score'] = sem.get('confidence_score', 0.0)
            row['description'] = sem.get('description', '')
            row['source'] = sem.get('source', '')
            row['validated'] = 0
            row['validated_by'] = ''
            row['validated_at'] = ''

        print(f'SemanticModel: {len(matched)} rule-based, {len(llm_results)} llm — {len(rows_to_insert)} rows written')
        return rows_to_insert
    
    
    def migrate(dbs_path=None):
        """
        Adds semantic columns to existing LanceDB tables if missing.
        Safe to run multiple times — skips columns that already exist.
        """
        db = LanceConnectionFactory.get(dbs_path)
    
        SEMANTIC_INDEXES = ['column_version', 'semantic_concept', 'pipeline', 'table_name', 'original_column_name', 'is_deleted']
    
        try:
            tbl = db.open_table('column_catalog')
            existing_cols = tbl.schema.names
            existing_indexes = {idx.name for idx in tbl.list_indices()}
    
            if 'embedding' not in existing_cols or tbl.schema.field('embedding').type != pa.list_(pa.float32(), EMBEDDING_DIMS):
                data = tbl.to_arrow()
                if 'embedding' in data.schema.names:
                    data = data.remove_column(data.schema.get_field_index('embedding'))
                data = data.append_column(
                    pa.field('embedding', pa.list_(pa.float32(), EMBEDDING_DIMS)),
                    pa.array([None] * len(data), type=pa.list_(pa.float32(), EMBEDDING_DIMS))
                )
                db.drop_table('column_catalog')
                tbl = db.create_table('column_catalog', data)

            for col, expr in SEMANTIC_ONLY_FIELDS.items():
                if col not in existing_cols:
                    tbl.add_columns({col: expr})
    
            for col in SEMANTIC_INDEXES:
                if col not in existing_indexes:
                    tbl.create_scalar_index(col)
    
        except Exception as e:
            print(f'column_catalog not found — skipping: {str(e)}')


    @staticmethod
    def get_embeddings(rows: list[dict], pipeline = None) -> list[dict]:
        """
        Generates vector embeddings for the semantic model using fastembed locally.
        """
        if not rows: return rows
        texts = [f"Pipeline: {r['pipeline'] if pipeline == None else pipeline} | Table: {r['table_name']} | Column {r['column_name']} | Concept: {r['semantic_concept']} | Description {r['description']}" for r in rows]

        try:
            model = SemanticModel._get_embedding_model()
            embeddings = list(model.embed(texts))

            for row, embedding in zip(rows, embeddings):
                row['embedding'] = embedding.tolist()

        except Exception as e:
            print(f'Embedding failed: {str(e)} — storing null embeddings')
            for row in rows:
                row['embedding'] = None

        return rows
    

    @staticmethod
    def _get_embedding_model():
        if SemanticModel._embedding_model is None:
            from fastembed import TextEmbedding
            from fastembed.common.model_description import PoolingType, ModelSource
            TextEmbedding.add_custom_model(
                model=EMBEDDING_MODEL, pooling=PoolingType.MEAN, normalization=True,
                sources=ModelSource(hf=EMBEDDING_MODEL), dim=EMBEDDING_DIMS,
                model_file="onnx/model.onnx",
            )
            SemanticModel._embedding_model = TextEmbedding(EMBEDDING_MODEL)
        return SemanticModel._embedding_model
    

    @staticmethod
    def save_semantic_updates(rows: list[dict], pipeline: str, table_name: str, namespace: str) -> int:
        from datetime import datetime

        if not rows:
            return 0

        try:
            con = SemanticModel._get_duckdb_conn()
            tbl = SemanticModel._get_table()

            original_pipeline_name = pipeline
            namespace = namespace.replace('-','_')
            pipeline = f'{namespace}_at_{pipeline}'

            existing_rows = con.execute(f"""
                SELECT * FROM column_catalog
                WHERE pipeline = '{pipeline}' AND table_name = '{table_name}' AND namespace = '{namespace}'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY pipeline, table_name, original_column_name
                    ORDER BY ingested_at DESC
                ) = 1
            """).fetchall()

            col_names = [d[0] for d in con.description]
            existing_map = {
                row[col_names.index('original_column_name')]: dict(zip(col_names, row))
                for row in existing_rows
            }

            rows_to_insert = []
            for row in rows:
                existing_row = existing_map.get(row['name'], {})

                semantic_changed = row['semantic'] != existing_row.get('semantic_concept', '')
                description_changed = row.get('description', '') != existing_row.get('description', '')

                if not semantic_changed and not description_changed:
                    continue

                entry = {
                    **existing_row,
                    'semantic_concept': row['semantic'],
                    'confidence_score': row['confidence'],
                    'source': row['sem_source'],
                    'validated': row['validated'],
                    'description': row['description'],
                    'validated_by': '',
                    'validated_at': datetime.utcnow().isoformat() if row['validated'] else '',
                    'ingested_at': datetime.utcnow().isoformat(),
                    'embedding': SemanticModel.get_embeddings([{
                                    'semantic_concept': row['semantic'],
                                    'description': row.get('description', existing_row.get('description', '')),
                                    'pipeline': original_pipeline_name,
                                    'column_name': row['column_name'],
                                    'table_name': row['table_name'],
                                }])[0]['embedding'],
                }
                rows_to_insert.append(entry)

            if rows_to_insert:
                column_names_to_delete = ", ".join(f"'{r['original_column_name']}'" for r in rows_to_insert)
                tbl.delete(f"pipeline = '{pipeline}' AND original_column_name IN ({column_names_to_delete})")

                tbl.add(rows_to_insert)
                print(f'{len(rows_to_insert)} semantic updates saved')

            return { 'error': False, 'result': f'{len(rows_to_insert)} semantic updates saved' }
        
        except Exception as err:
            error = 'Error while updating semantic model: '+str(err)
            print('Error on updating semantic: ', error)
            print(error)
            return { 'error': True, 'result': { 'result': error } } 
