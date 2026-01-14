import duckdb
from config_path_00 import DB_CAMINHO

def main():
    con = duckdb.connect(str(DB_CAMINHO))
    con.execute("CREATE SCHEMA IF NOT EXISTS silver_schema;")

    # Orders - datas e atraso em dias
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
                WHEN order_delivered_customer_date IS NULL OR order_estimated_delivery_date IS NULL THEN NULL
                ELSE DATE_DIFF(
                    'day',
                    CAST(order_estimated_delivery_date AS DATE),
                    CAST(order_delivered_customer_date AS DATE)
                )
            END AS atraso_dias
        FROM bronze_schema.orders;
    """)

    # Order items - criação da coluna receita - Preço + Frete
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
            p.product_id,
            p.product_category_name,
            CASE
                WHEN t.product_category_name_english IS NULL THEN p.product_category_name
                ELSE t.product_category_name_english
            END AS product_category_name_english
        FROM bronze_schema.products p
        LEFT JOIN bronze_schema.category_translate t
            ON p.product_category_name = t.product_category_name;
    """)

    print("\nChecagem se consulta está ok:")
    print(con.execute("SELECT * FROM silver_schema.orders;").fetchdf())
    print(con.execute("SELECT * FROM silver_schema.order_items;").fetchdf())
    print(con.execute("SELECT * FROM silver_schema.customers;").fetchdf())
    print(con.execute("SELECT * FROM silver_schema.products;").fetchdf())

    con.close()
    print("\nCamada Bronze concluída com sucesso!")

if __name__ == "__main__":
    main()
