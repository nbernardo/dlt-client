import duckdb
import threading
from fastembed import TextEmbedding


def install_duckdb_extensions():
    print("Installing DuckDB lance extension...")
    con = duckdb.connect()
    con.execute("INSTALL lance FROM community")
    con.close()
    print("✅ DuckDB lance extension installed")


def download_embedding_model():
    print("Installing fastembed model intfloat/multilingual-e5-small (~120MB)...")
    from fastembed.common.model_description import PoolingType, ModelSource
    TextEmbedding.add_custom_model(
        model="intfloat/multilingual-e5-small",
        pooling=PoolingType.MEAN,
        normalization=True,
        sources=ModelSource(hf="intfloat/multilingual-e5-small"),
        dim=384,
        model_file="onnx/model.onnx",
    )
    TextEmbedding("intfloat/multilingual-e5-small")
    print("✅ fastembed model intfloat/multilingual-e5-small installed")


answer = input(
    "\nDo you want to install intfloat/multilingual-e5-small for multilingual vector embedding and semantic search?\n"
    "This model supports 100+ languages with 384 dims (~120MB download).\n"
    "Without it, vector embedding and semantic search will not be available.\n"
    "[y/N]: "
).strip().lower()
install_embedding = answer == 'y'

threads = [threading.Thread(target=install_duckdb_extensions)]
if install_embedding:
    threads.append(threading.Thread(target=download_embedding_model))

for t in threads:
    t.start()
for t in threads:
    t.join()

if not install_embedding:
    print("⚠️  Embedding model not installed — vector embedding and semantic search unavailable")

print("✅ All extensions installed")