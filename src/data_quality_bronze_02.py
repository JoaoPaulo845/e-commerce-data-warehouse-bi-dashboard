import duckdb
from config_path_00 import DB_CAMINHO

# ==============================================
# ========= Análise de dados (Bronze) ==========
# ==============================================

# Objetivo: mostrar as inconsistências para tratar na camada silver

def main():
    con = duckdb.connect(str(DB_CAMINHO))

    # Tamanho das tabelas - contagem de linhas

    print("\nVolume de dados (linhas):")
    print(con.execute("""
        (SELECT 'orders' AS tabela, COUNT(*) AS linhas FROM bronze_schema.orders)
        UNION ALL
        (SELECT 'order_items', COUNT(*) FROM bronze_schema.order_items)
        UNION ALL
        (SELECT 'customers', COUNT(*) FROM bronze_schema.customers)
        UNION ALL
        (SELECT 'products', COUNT(*) FROM bronze_schema.products)
        UNION ALL
        (SELECT 'category_translate', COUNT(*) FROM bronze_schema.category_translate)
    """).fetchdf()) # fetchdf() retorna um dataframe, o que torna mais fácil a visualização

    # A partir daqui selecionarei as colunas com mais impacto no negócio e que podem ser usadas em joins posteriormente

    # Chaves nulas

    print("\nContagem de nulos:")

    print("\nOrders:")
    print(con.execute("""
        SELECT
            SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS order_id_nulos,
            SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS customer_id_nulos
                FROM bronze_schema.orders;
    """).fetchdf())

    print("\nOrder_items:")
    print(con.execute("""
        SELECT 
            SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS order_id_nulos,
            SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS product_id_nulos,
            SUM(CASE WHEN order_item_id IS NULL THEN 1 ELSE 0 END) AS order_item_id_nulos
                FROM bronze_schema.order_items;
    """).fetchdf())

    # Duplicados por chave 

    print("\nChecagem de duplicados:")

    print("\nOrders - duplicados por order_id:")
    print(con.execute("""
        SELECT
            COUNT(*) AS linhas,
            COUNT(DISTINCT order_id) AS order_ids_distintos,
            (COUNT(*) - COUNT(DISTINCT order_id)) AS duplicados
                FROM bronze_schema.orders;
    """).fetchdf())

    print("\nOrder_items duplicados por order_id & order_item_id:")
    print(con.execute("""
        SELECT
            COUNT(*) AS linhas,
            COUNT(DISTINCT order_id || '-' || order_item_id) AS chaves_distintas,
            (COUNT(*) - COUNT(DISTINCT order_id || '-' || order_item_id)) AS duplicados
                FROM bronze_schema.order_items;
    """).fetchdf())

    # Valores negativos - não podem ser usados para preço ou frete

    print("\nChecagem de valores negativos:")
    print(con.execute("""
        SELECT
            SUM(CASE WHEN price < 0 THEN 1 ELSE 0 END) AS price_negativo,
            SUM(CASE WHEN freight_value < 0 THEN 1 ELSE 0 END) AS frete_negativo
        FROM bronze_schema.order_items;
    """).fetchdf())


    con.close()

    print("Análise para a etapa Silver realizada!.")

if __name__ == "__main__":
    main()

# Output

"""
Volume de dados (linhas):
               tabela  linhas
0              orders   99441
1         order_items  112650
2           customers   99441
3            products   32951
4  category_translate      71

Contagem de nulos:

Orders:
   order_id_nulos  customer_id_nulos
0             0.0                0.0

Order_items:
   order_id_nulos  product_id_nulos  order_item_id_nulos
0             0.0               0.0                  0.0

Checagem de duplicados:

Orders - duplicados por order_id:
   linhas  order_ids_distintos  duplicados
0   99441                99441           0

Order_items duplicados por order_id & order_item_id:
   linhas  chaves_distintas  duplicados
0  112650            112650           0

Checagem de valores negativos:
   price_negativo  frete_negativo
0             0.0             0.0
Análise para a etapa Silver realizada!.

"""