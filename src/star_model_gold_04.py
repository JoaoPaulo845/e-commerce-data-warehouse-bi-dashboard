import duckdb
from config_path_00 import DB_CAMINHO

# ========================================
# ================ GOLD ==================
# ========================================

# Objetivo: criar o modelo estrela pronto para ser usado no Power BI.
# Aqui criarei 4 tabelas - Dimensão das datas, Dimensão dos clientes, Dimensão dos produtos, Vendas realizadas

def main():
    con = duckdb.connect(str(DB_CAMINHO))

    # Criar Schema Gold

    con.execute("CREATE SCHEMA IF NOT EXISTS gold_schema;")

    # DIM_DATE
    # Dimensão da data de compra
    # A chave date_key no formato YYYYMMDD ajuda a ligar com fact_sales.

    con.execute("""
        CREATE OR REPLACE TABLE gold_schema.dim_date AS
        SELECT DISTINCT
            CAST(strftime(CAST(purchase_ts AS DATE), '%Y%m%d') AS INTEGER) AS date_key,
            CAST(purchase_ts AS DATE) AS data,
            EXTRACT(year FROM CAST(purchase_ts AS DATE)) AS ano,
            EXTRACT(month FROM CAST(purchase_ts AS DATE)) AS mes,
            EXTRACT(day FROM CAST(purchase_ts AS DATE)) AS dia
        FROM silver_schema.orders
        WHERE purchase_ts IS NOT NULL
        ORDER BY data;
    """)

    # DIM_CUSTOMER
    # Dimensão do cliente

    con.execute("""
        CREATE OR REPLACE TABLE gold_schema.dim_customer AS
        SELECT DISTINCT
            customer_id,
            customer_unique_id,
            customer_city,
            customer_state
        FROM silver_schema.customers;
    """)

    # DIM_PRODUCT
    # Dimensão doproduto

    con.execute("""
        CREATE OR REPLACE TABLE gold_schema.dim_product AS
        SELECT DISTINCT
            product_id,
            product_category_name,
            product_category_name_english
        FROM silver_schema.products;
    """)


    # Sales
    # Tabela de vendas com 1 linha por item do pedido.
    # Junta orders com order_items

    con.execute("""
        CREATE OR REPLACE TABLE gold_schema.sales AS
        SELECT
            B.order_id,
            B.order_item_id,
            A.customer_id,
            B.product_id,
            CAST(strftime(CAST(A.purchase_ts AS DATE), '%Y%m%d') AS INTEGER) AS purchase_date_key,
            CAST(A.purchase_ts AS DATE) AS purchase_date,
            A.order_status,
            A.atraso_dias,
            B.receita_item,
            B.frete_item,
            B.receita_total_item
        FROM silver_schema.orders A
        INNER JOIN silver_schema.order_items B
            ON A.order_id = B.order_id;
    """)

    # Testando consultas!
    print("\nSelect para checagem:")
    print(con.execute("SELECT * FROM gold_schema.dim_date LIMIT 3;").fetchdf())
    print(con.execute("SELECT * FROM gold_schema.dim_customer LIMIT 3;").fetchdf())
    print(con.execute("SELECT * FROM gold_schema.dim_product LIMIT 3;").fetchdf())
    print(con.execute("SELECT * FROM gold_schema.sales LIMIT 3;").fetchdf())

    con.close()
    print("\nCamada Gold criada com sucesso!")

if __name__ == "__main__":
    main()

# Output

""" 
Select para checagem:
   date_key       data   ano  mes  dia
0  20160904 2016-09-04  2016    9    4
1  20160905 2016-09-05  2016    9    5
2  20160913 2016-09-13  2016    9   13
                        customer_id                customer_unique_id customer_city customer_state
0  d387341bbce5ab96e3647bb0e8d0b55a  91e6fd64694fa9a828270d442ba88e03          embu             SP
1  9e0628923d9fe029c079eb11ae72d241  dc643b60537f44f35b2f91610ea64153   eugenopolis             MG
2  82a5aaed30c2e07b5b0e4cb5dfcc332d  0d77d20b2c7c92602b0c4ec4fe5997af      cidreira             RS
                         product_id  product_category_name product_category_name_english
0  8ba4f2a4ae695d26e5626c1bf710975e  utilidades_domesticas                    housewares
1  20e37962088a4061d3578def47a8648b       moveis_decoracao               furniture_decor
2  fdeb34a9f03fea7c3937dd62d1d0287e             cool_stuff                    cool_stuff
                           order_id  order_item_id  ... frete_item receita_total_item
0  00010242fe8c5a6d1ba2dd792cb16214              1  ...      13.29              72.19
1  00018f77f2f0320c557190d7a144bdd3              1  ...      19.93             259.83
2  000229ec398224ef6ca0657da4fc703e              1  ...      17.87             216.87

[3 rows x 11 columns]

Camada Gold criada com sucesso!

"""