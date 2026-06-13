import duckdb
from duckdb import DuckDBPyConnection

class DataDictionary:
  ...

  def create_disctionary_map_table(con: DuckDBPyConnection):
    query = '''
      CREATE TABLE IF NOT EXISTS dwhperformance_meta.field_dictionary (
          table_name VARCHAR,
          field_name VARCHAR,
          translation VARCHAR,
          description VARCHAR,
          status VARCHAR,
          lang VARCHAR,
          created_date TIMESTAMP,
          updated_date TIMESTAMP,
          PRIMARY KEY (table_name, field_name, lang)
      );
    '''
    
    try:
      con.execute(query)

    except Exception as err:
      print(f'Error while creating dictionary_table -> {str(err)}')
      ...

  
  def upsert_dictionary(db_path, values):
    con = duckdb.connect(db_path)

    try:
      table = 'dwhperformance_meta.field_dictionary'
      con.executemany(
          f"""
          INSERT INTO {table} ( table_name, field_name, translation, description, status, lang, created_date, updated_date )
          VALUES ( ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP )
          ON CONFLICT (table_name, field_name, lang)
          DO UPDATE SET
              translation = EXCLUDED.translation, description = EXCLUDED.description, status = EXCLUDED.status, updated_date = get_current_timestamp()
          """,
          [(r['table'], r['name'], r['trans'], r['desc'], r.get('status',True), r.get('lang', 'PT')) for r in values]
      )
    
    except Exception as err:
      print(f'Error while updating the dictionary: {str(err)}')

    finally:
      con.close()