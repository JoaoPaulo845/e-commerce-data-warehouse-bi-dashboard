from pathlib import Path

# Caminho raiz do projeto | a variável __file__ guarda a pasta atual do projeto, a função .parests() é para voltar 1 caminho, que leva no caminho no projeto base
PROJETO_ROOT = Path(__file__).resolve().parents[1]

# Onde ficam os CSV Base - Bronze
DIRETORIO_RAW = PROJETO_ROOT / "data" / "raw"

# Banco DuckDB
DB_CAMINHO = PROJETO_ROOT / "outputs" / "datawarehouse.duckdb"
