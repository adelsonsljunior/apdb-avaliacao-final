import pandas as pd
import psycopg2

DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "olist"
DB_USER = "postgres"
DB_PASS = "123456"

# Variáveis globais para armazenar as chaves primárias carregadas
# Isso simula a existência das PKs no banco de dados após a inserção
PK_SETS = {
    "product_category_name_translation": set(),
    "olist_seller": set(),
    "olist_customer": set(),
    "olist_product": set(),
    "olist_order": set(),
}


def insert_values(
    query: str, data: any, pk_column: str = None, pk_set_name: str = None
):
    """Insere múltiplos valores usando executemany."""
    try:
        with psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
        ) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, data)

                print(f"{cursor.rowcount} linhas inseridas com sucesso!")

                # Armazena as PKs inseridas para uso no filtro de tabelas filhas
                if pk_column and pk_set_name:
                    # CORREÇÃO: A PK (customer_id) é a primeira coluna na tupla de dados
                    # A função itertuples do pandas retorna os dados na ordem das colunas
                    new_pks = {row[0] for row in data}
                    PK_SETS[pk_set_name].update(new_pks)
                    print(f"Total de PKs em {pk_set_name}: {len(PK_SETS[pk_set_name])}")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erro ao inserir múltiplos dados: {error}")


def insert_product_category_name_translation():
    print("\n--- Carga de Dados: product_category_name_translation ---")

    df = pd.read_csv("./datasets/product_category_name_translation.csv")
    data = list(df.itertuples(index=False, name=None))

    query = """
        INSERT INTO product_category_name_translation(
            product_category_name, product_category_name_english
        ) 
        VALUES (%s, %s);
    """

    # Armazena a PK (product_category_name)
    insert_values(
        query=query,
        data=data,
        pk_column="product_category_name",
        pk_set_name="product_category_name_translation",
    )


def insert_olist_sellers():
    print("\n--- Carga de Dados: olist_seller ---")

    df = pd.read_csv("./datasets/olist_sellers_dataset.csv")

    df["seller_zip_code_prefix"] = df["seller_zip_code_prefix"].astype(str).str.zfill(5)

    data = list(df.itertuples(index=False, name=None))

    query = """
        INSERT INTO olist_seller(
            seller_id, seller_zip_code_prefix, seller_city, seller_state
        )
        VALUES(%s, %s, %s, %s)
    """

    # Armazena a PK (seller_id)
    insert_values(
        query=query, data=data, pk_column="seller_id", pk_set_name="olist_seller"
    )


def insert_olist_customers():
    print("\n--- Carga de Dados: olist_customer ---")

    df = pd.read_csv("./datasets/olist_customers_dataset.csv")

    df["customer_zip_code_prefix"] = (
        df["customer_zip_code_prefix"].astype(str).str.zfill(5)
    )

    df = df[
        [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ]
    ]

    data = list(df.itertuples(index=False, name=None))

    query = """
        INSERT INTO olist_customer(
            customer_id, customer_unique_id, customer_zip_code_prefix, 
            customer_city, customer_state
        )
        VALUES(%s, %s, %s, %s, %s)
        ON CONFLICT (customer_unique_id) DO NOTHING
    """

    # Armazena a PK (customer_id)
    insert_values(
        query=query, data=data, pk_column="customer_id", pk_set_name="olist_customer"
    )


def insert_olist_geolocation():
    print("\n--- Carga de Dados: olist_geolocation ---")

    df = pd.read_csv("./datasets/olist_geolocation_dataset.csv")

    df["geolocation_zip_code_prefix"] = (
        df["geolocation_zip_code_prefix"].astype(str).str.zfill(5)
    )

    data = list(df.itertuples(index=False, name=None))

    query = """
        INSERT INTO olist_geolocation(
            geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
            geolocation_city, geolocation_state
        )
        VALUES(%s, %s, %s, %s, %s)
    """

    insert_values(query=query, data=data)


def insert_olist_products():
    print("\n--- Carga de Dados: olist_product (Com Filtro de FK) ---")

    df = pd.read_csv("./datasets/olist_products_dataset.csv")

    int_columns = [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]

    df[int_columns] = df[int_columns].fillna(0).astype(int)

    # 1. TRATAMENTO DE DADOS ÓRFÃOS: Filtra produtos cuja categoria não existe na tabela de tradução
    valid_categories = PK_SETS["product_category_name_translation"]

    # Remove linhas onde a categoria é NaN OU vazia
    df = df[df["product_category_name"].notna() & (df["product_category_name"] != "")]

    # Filtra as categorias que existem na tabela pai
    df_filtered = df[df["product_category_name"].isin(valid_categories)]

    removed_count = len(df) - len(df_filtered)
    if removed_count > 0:
        print(
            f"AVISO: {removed_count} produtos removidos por terem 'product_category_name' órfão."
        )

    # Reordenar colunas para corresponder à query: product_id é a primeira
    df_filtered = df_filtered[
        [
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ]
    ]

    data = list(df_filtered.itertuples(index=False, name=None))

    query = """
        INSERT INTO olist_product(
            product_id, product_category_name, product_name_lenght, 
            product_description_lenght, product_photos_qty, product_weight_g, 
            product_length_cm, product_height_cm, product_width_cm
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Armazena a PK (product_id)
    insert_values(
        query=query, data=data, pk_column="product_id", pk_set_name="olist_product"
    )


def insert_olist_orders():
    print("\n--- Carga de Dados: olist_order (Com Filtro de FK) ---")

    df = pd.read_csv("./datasets/olist_orders_dataset.csv")

    df = df.where(pd.notnull(df), None)

    # 1. TRATAMENTO DE DADOS ÓRFÃOS: Filtra pedidos cujo cliente não existe
    valid_customers = PK_SETS["olist_customer"]

    df_filtered = df[df["customer_id"].isin(valid_customers)]

    removed_count = len(df) - len(df_filtered)
    if removed_count > 0:
        print(
            f"AVISO: {removed_count} pedidos removidos por terem 'customer_id' órfão."
        )

    # Reordenar colunas para corresponder à query: order_id é a primeira
    df_filtered = df_filtered[
        [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ]
    ]

    data = list(df_filtered.itertuples(index=False, name=None))

    query = """
        INSERT INTO olist_order(
            order_id, customer_id, order_status, order_purchase_timestamp, 
            order_approved_at, order_delivered_carrier_date, 
            order_delivered_customer_date, order_estimated_delivery_date
        )
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Armazena a PK (order_id)
    insert_values(
        query=query, data=data, pk_column="order_id", pk_set_name="olist_order"
    )


def insert_olist_order_items():
    print("\n--- Carga de Dados: olist_order_item (Com Filtro de FK) ---")

    df = pd.read_csv("./datasets/olist_order_items_dataset.csv")

    # 1. TRATAMENTO DE DADOS ÓRFÃOS: Filtra itens de pedido
    valid_orders = PK_SETS["olist_order"]
    valid_products = PK_SETS["olist_product"]
    valid_sellers = PK_SETS["olist_seller"]

    df_filtered = df[
        df["order_id"].isin(valid_orders)
        & df["product_id"].isin(valid_products)
        & df["seller_id"].isin(valid_sellers)
    ]

    removed_count = len(df) - len(df_filtered)
    if removed_count > 0:
        print(f"AVISO: {removed_count} itens de pedido removidos por terem FKs órfãs.")

    # Reordenar colunas para corresponder à query
    df_filtered = df_filtered[
        [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ]
    ]

    data = list(df_filtered.itertuples(index=False, name=None))

    query = """
        INSERT INTO olist_order_item(
            order_id, order_item_id, product_id, seller_id, 
            shipping_limit_date, price, freight_value
        )
        VALUES(%s, %s, %s, %s, %s, %s, %s)    
    """

    # A PK é composta (order_id, order_item_id), não precisamos armazenar
    insert_values(query=query, data=data)


def insert_olist_order_payments():
    print("\n--- Carga de Dados: olist_order_payment (Com Filtro de FK) ---")

    df = pd.read_csv("./datasets/olist_order_payments_dataset.csv")

    # 1. TRATAMENTO DE DADOS ÓRFÃOS: Filtra pagamentos cujo pedido não existe
    valid_orders = PK_SETS["olist_order"]

    df_filtered = df[df["order_id"].isin(valid_orders)]

    removed_count = len(df) - len(df_filtered)
    if removed_count > 0:
        print(
            f"AVISO: {removed_count} pagamentos removidos por terem 'order_id' órfão."
        )

    # Reordenar colunas para corresponder à query
    df_filtered = df_filtered[
        [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ]
    ]

    data = list(df_filtered.itertuples(index=False, name=None))

    query = """
        INSERT INTO olist_order_payment(
            order_id, payment_sequential, payment_type, 
            payment_installments, payment_value
        )
        VALUES(%s, %s, %s, %s, %s)    
    """

    insert_values(query=query, data=data)


def insert_olist_order_reviews():
    print("\n--- Carga de Dados: olist_order_review (Com Filtro de FK) ---")

    df = pd.read_csv("./datasets/olist_order_reviews_dataset.csv")

    df = df.where(pd.notnull(df), None)

    # 1. TRATAMENTO DE DADOS ÓRFÃOS: Filtra avaliações cujo pedido não existe
    valid_orders = PK_SETS["olist_order"]

    df_filtered = df[df["order_id"].isin(valid_orders)]

    removed_count = len(df) - len(df_filtered)
    if removed_count > 0:
        print(
            f"AVISO: {removed_count} avaliações removidas por terem 'order_id' órfão."
        )

    # Reordenar colunas para corresponder à query
    df_filtered = df_filtered[
        [
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
        ]
    ]

    data = list(df_filtered.itertuples(index=False, name=None))

    query = """
        INSERT INTO olist_order_review(
            review_id, order_id, review_score, review_comment_title, 
            review_comment_message, review_creation_date, review_answer_timestamp
        )
        VALUES(%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (review_id) DO NOTHING 
    """

    insert_values(query=query, data=data)


def main():
    print("--- INÍCIO DO PROCESSO DE CARREGAMENTO E TRATAMENTO DE DADOS ---")

    insert_product_category_name_translation()
    insert_olist_sellers()
    insert_olist_customers()
    insert_olist_geolocation()
    insert_olist_products()  # Depende de product_category_name_translation
    insert_olist_orders()  # Depende de olist_customer
    insert_olist_order_items()  # Depende de olist_order, olist_product, olist_seller
    insert_olist_order_payments()  # Depende de olist_order
    insert_olist_order_reviews()  # Depende de olist_order

    print("\n--- PROCESSO CONCLUÍDO COM SUCESSO ---")


if __name__ == "__main__":
    main()
