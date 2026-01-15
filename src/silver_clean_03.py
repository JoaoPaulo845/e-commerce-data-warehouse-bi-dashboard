import duckdb
from config_path_00 import DB_CAMINHO

# ==========================================
# =============== SILVER ===================
# ==========================================

# Objetivo: padronizar e enriquecer os dados do Bronze para ficarem prontos para o Gold (BI).
# Como o Bronze já está bem limpo, não será necessário tratamento da qualidade de dados.
# Aqui irei uma tabela tratada para cada tabela que será usada no Dashboard
# Irei criar colunas para facilitar a criação do dashboard, padronizar a tradução e padronizar os tipos de datas. 

def main():
    con = duckdb.connect(str(DB_CAMINHO))

    # Criar Schema Silver
    con.execute("CREATE SCHEMA IF NOT EXISTS silver_schema;")

    # Padronizar datas e criar atrasos
    con.execute("""
        CREATE OR REPLACE TABLE silver_schema.orders AS
        SELECT
            order_id,
            customer_id,
            order_status,
            CAST(order_purchase_timestamp AS TIMESTAMP) AS purchase_ts,
            CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_customer_ts,
            CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_ts,
            CASE
                WHEN order_delivered_customer_date IS NULL OR order_estimated_delivery_date IS NULL 
                THEN NULL
                    ELSE DATE_DIFF('day', CAST(order_estimated_delivery_date AS DATE), CAST(order_delivered_customer_date AS DATE)
                )
            END AS atraso_dias
        FROM bronze_schema.orders;
    """)

    # Order_items
    con.execute("""
        CREATE OR REPLACE TABLE silver_schema.order_items AS
        SELECT
            order_id,
            order_item_id,
            product_id,
            price AS receita_item,
            freight_value AS frete_item,
            (price + freight_value) AS receita_total_item
        FROM bronze_schema.order_items;
    """)

    # Customers 
    con.execute("""
        CREATE OR REPLACE TABLE silver_schema.customers AS
        SELECT
            customer_id,
            customer_unique_id,
            customer_city,
            customer_state
        FROM bronze_schema.customers;
    """)

    # Products com tradução 
    con.execute("""
        CREATE OR REPLACE TABLE silver_schema.products AS
        SELECT
            A.product_id,
            A.product_category_name,
            CASE
                WHEN B.product_category_name_english IS NULL THEN A.product_category_name
                ELSE B.product_category_name_english
            END AS product_category_name_english
        FROM bronze_schema.products A
        LEFT JOIN bronze_schema.category_translate B
            ON A.product_category_name = B.product_category_name;
    """)

    # Testando consultas!
    print("\nSelect para checagem:")
    print(con.execute("SELECT * FROM silver_schema.orders limit 3").fetchdf())
    print(con.execute("SELECT * FROM silver_schema.order_items limit 3").fetchdf())
    print(con.execute("SELECT * FROM silver_schema.customers limit 3").fetchdf())
    print(con.execute("SELECT * FROM silver_schema.products limit 3").fetchdf())

    con.close()
    print("\nCamada Silver criada com sucesso!")

if __name__ == "__main__":
    main()

# Output:

"""
Select para checagem:
                           order_id                       customer_id  ... estimated_delivery_ts atraso_dias
0  e481f51cbdc54678b7cc49136f2d6af7  9ef432eb6251297304e76186b10a928d  ...            2017-10-18          -8
1  53cdb2fc8bc7dce0b6741e2150273451  b0830fb4747a6c6d20dea0b8c802d7ef  ...            2018-08-13          -6
2  47770eb9100c2d0c44946d9cf07ec65d  41ce2a54c0b03bf3443c3d931a367089  ...            2018-09-04         -18

[3 rows x 7 columns]
                           order_id  order_item_id  ... frete_item  receita_total_item
0  00010242fe8c5a6d1ba2dd792cb16214              1  ...      13.29               72.19
1  00018f77f2f0320c557190d7a144bdd3              1  ...      19.93              259.83
2  000229ec398224ef6ca0657da4fc703e              1  ...      17.87              216.87

[3 rows x 6 columns]
                        customer_id                customer_unique_id          customer_city customer_state
0  06b8999e2fba1a1fbc88172c00ba8bc7  861eff4711a542e4b93843c6dd7febb0                 franca             SP
1  18955e83d337fd6b2def6b18a428ac77  290c77bc529b7ac935b93aa66c333dc3  sao bernardo do campo             SP
2  4e7b3e00288586ebd08712fdd0374a03  060e732b5b29e8181a18229c7b0b2b5e              sao paulo             SP
                         product_id product_category_name product_category_name_english
0  1e9e8ef04dbcff4541ed26657ea517e5            perfumaria                     perfumery
1  3aa071139cb16b67ca9e5dea641aaa2f                 artes                           art
2  96bd76ec8810374ed1b65e291975717f         esporte_lazer                sports_leisure
\Camada Silver criada com sucesso!

"""