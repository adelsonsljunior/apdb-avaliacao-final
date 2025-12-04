import pandas as pd
import psycopg2


DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "olist"
DB_USER = "postgres"
DB_PASS = "123456"


def insert_values(query: str, data: any):
    try:
        with psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
        ) as conn:
            with conn.cursor() as cursor:
                # O executemany vai inserir a lista de tuplas
                cursor.executemany(query, data)

                print(f"{cursor.rowcount} linhas inseridas com sucesso!")
                # (Commit automático ao sair do 'with')

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erro ao inserir múltiplos dados: {error}")
        # (Rollback automático ao sair do 'with' com erro)


def insert_product_category_name_translation():
    print("Começando carga de dados da Tabela: product_category_name_translation")

    df = pd.read_csv("./datasets/product_category_name_translation.csv")
    data = list(df.itertuples(index=False, name=None))

    query = """
        INSERT INTO product_category_name_translation(
            product_category_name, product_category_name_english
        ) 
        VALUES (%s, %s);
    """

    insert_values(query=query, data=data)


def insert_olist_sellers():
    print("Começando carga de dados da Tabela: olist_seller")

    df = pd.read_csv("./datasets/olist_sellers_dataset.csv")

    # Converter o atributo seller_zip_code_prefix para string
    # Em caso de ter 4 caráteres, é adicionado um 0 na frente
    df["seller_zip_code_prefix"] = df["seller_zip_code_prefix"].astype(str).str.zfill(5)

    data = list(df.itertuples(index=False, name=None))

    query = """
        INSERT INTO olist_seller(
            seller_id, seller_zip_code_prefix, seller_city, seller_state
        )
        VALUES(%s, %s, %s, %s)
    """

    insert_values(query=query, data=data)


def insert_olist_products():
    print("Começando carga de dados da Tabela: olist_product")

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

    # Substitui NaN por 0 (ou por None se preferir manter null no Postgres)
    df[int_columns] = df[int_columns].fillna(0)

    # Converte tudo para inteiro
    df[int_columns] = df[int_columns].astype(int)

    data = list(df.itertuples(index=False, name=None))

    query = """
        INSERT INTO olist_product(
            product_id, product_category_name, product_name_lenght, 
            product_description_lenght, product_photos_qty, product_weight_g, 
            product_length_cm, product_height_cm, product_width_cm
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    insert_values(query=query, data=data)


def insert_olist_customers():
    print("Começando carga de dados da Tabela: olist_customer")

    df = pd.read_csv("./datasets/olist_customers_dataset.csv")

    # Converter o atributo customer_zip_code_prefix para string
    # Em caso de ter 4 caráteres, é adicionado um 0 na frente
    df["customer_zip_code_prefix"] = (
        df["customer_zip_code_prefix"].astype(str).str.zfill(5)
    )

    data = list(df.itertuples(index=False, name=None))

    query = """
        INSERT INTO olist_customer(
            customer_id, customer_unique_id, customer_zip_code_prefix, 
            customer_city, customer_state
        )
        VALUES(%s, %s, %s, %s, %s)
        ON CONFLICT (customer_unique_id) DO NOTHING
    """
    # ON CONFLICT (customer_unique_id) DO NOTHING - ignore duplicatas
    # DETAIL: Key (customer_unique_id)=(b6c083700ca8c135ba9f0f132930d4e8) already exists.

    insert_values(query=query, data=data)


def insert_olist_geolocation():
    print("Começando carga de dados da Tabela: olist_geolocation")

    df = pd.read_csv("./datasets/olist_geolocation_dataset.csv")

    # Mesmo com aspas no campo do csv, o pandas está lendo como inteiro
    # Então é preciso converter pra string
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


def insert_olist_orders():
    print("Começando carga de dados da Tabela: olist_order")

    df = pd.read_csv("./datasets/olist_orders_dataset.csv")

    # Converte NaN para None evitando problemas com timestamps
    df = df.where(pd.notnull(df), None)

    data = list(df.itertuples(index=False, name=None))

    query = """
        INSERT INTO olist_order(
            order_id, customer_id, order_status, order_purchase_timestamp, 
            order_approved_at, order_delivered_carrier_date, 
            order_delivered_customer_date, order_estimated_delivery_date
        )
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
    """

    insert_values(query=query, data=data)


def insert_olist_order_items():
    print("Começando carga de dados da Tabela: olist_order_item")

    df = pd.read_csv("./datasets/olist_order_items_dataset.csv")

    data = list(df.itertuples(index=False, name=None))

    query = """
        INSERT INTO olist_order_item(
            order_id, order_item_id, product_id, seller_id, 
            shipping_limit_date, price, freight_value
        )
        VALUES(%s, %s, %s, %s, %s, %s, %s)    
    """

    insert_values(query=query, data=data)


def insert_olist_order_payments():
    print("Começando carga de dados da Tabela: olist_order_payment")

    df = pd.read_csv("./datasets/olist_order_payments_dataset.csv")

    data = list(df.itertuples(index=False, name=None))

    query = """
        INSERT INTO olist_order_payment(
            order_id, payment_sequential, payment_type, 
            payment_installments, payment_value
        )
        VALUES(%s, %s, %s, %s, %s)    
    """

    insert_values(query=query, data=data)


def insert_olist_order_reviews():
    print("Começando carga de dados da Tabela: olist_order_review")

    df = pd.read_csv("./datasets/olist_order_reviews_dataset.csv")

    # Converte NaN para None
    df = df.where(pd.notnull(df), None)

    data = list(df.itertuples(index=False, name=None))

    query = """
        INSERT INTO olist_order_review(
            review_id, order_id, review_score, review_comment_title, 
            review_comment_message, review_creation_date, review_answer_timestamp
        )
        VALUES(%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (review_id) DO NOTHING 
    """
    # ON CONFLICT (review_id) DO NOTHING - ignore duplicatas
    # DETAIL:  Key (review_id)=(3242cc306a9218d0377831e175d62fbf) already exists.

    insert_values(query=query, data=data)


if __name__ == "__main__":
    insert_product_category_name_translation()
    insert_olist_sellers()
    insert_olist_customers()
    insert_olist_geolocation()
    insert_olist_products()
    insert_olist_orders()
    insert_olist_order_items()
    insert_olist_order_payments()
    insert_olist_order_reviews()
