import duckdb
import pandas as pd

from config_path_00 import DIRETORIO_RAW, DB_CAMINHO

# ============================================
# ======== Ingestão de dados - Bronze ========
# ============================================

# Dados crus, da forma que veio no CSV
# O Objetivo é carregar os dados Crus sem tra.*

arquivos = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "category_translate": "product_category_name_translation.csv"
}


def main():
    conexao = duckdb.connect(str(DB_CAMINHO))

    # Criar schema bronze no duckdb
    conexao.execute("CREATE SCHEMA IF NOT EXISTS bronze_schema;")

    # Loop para adicionar todas as tabelas, função items() retorna chave e valor dataset
    for tabela, arquivo in arquivos.items():
        caminho = DIRETORIO_RAW / arquivo
        # Caso o arquivo não seja encontrado alerta e o código continua
        if not caminho.exists():
            print(f"Arquivo não encontrado: {caminho}")
            continue

        df = pd.read_csv(caminho)

    # Passa o DataFrame para o DuckDB e cria a tabela
        conexao.register("df_tmp", df)
        conexao.execute(f"CREATE OR REPLACE TABLE bronze_schema.{tabela} AS SELECT * FROM df_tmp;")
        conexao.unregister("df_tmp")

        print(f"bronze_schema.{tabela} carregado ({len(df)} linhas)")

    conexao.close()
    print(f"\nBanco DuckDB criado/atualizado em: {DB_CAMINHO}")

if __name__ == "__main__":
    main()

# Output:

"""
bronze_schema.orders carregado (99441 linhas)
bronze_schema.order_items carregado (112650 linhas)
bronze_schema.customers carregado (99441 linhas)
bronze_schema.products carregado (32951 linhas)
bronze_schema.category_translate carregado (71 linhas) 

"""