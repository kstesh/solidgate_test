import random
import uuid
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
import pandas as pd

currencies = ['USD', 'EUR', 'UAH', 'GBP', 'PLN']
ORDERS_NUMBER = 100


@dag(
    dag_id='generate_orders',
    description = "Generate data and write to ORDERS table",
    schedule = "*/10 * * * *",
    start_date = datetime(2025, 5, 16),
    catchup = False,
)
def generate_orders():

    # Function to generate a single order
    def generate_order():
        return (
            str(uuid.uuid4()),
            f"user{random.randint(1000, 9999)}@test.test",
            datetime.now() - timedelta(days=random.randint(0, 6), seconds=random.randint(0, 24*60*60)),
            round(random.uniform(10, 1000), 2),
            random.choice(currencies)
        )

    @task(task_id='generate_new_data',)
    def generate_new_data():
        logging.info(f"Generating {ORDERS_NUMBER} orders...")
        orders = [generate_order() for _ in range(ORDERS_NUMBER)]
        logging.info(f"{ORDERS_NUMBER} were generated!")
        return pd.DataFrame(orders, columns=['order_id', 'customer_email', 'order_date', 'amount', 'currency'])

    @task(task_id='insert_data_into_table',)
    def insert_data_into_table(df_to_upload: pd.DataFrame):
        logging.info(f"Inserting dato to Orders table...")

        if df_to_upload is None:
            logging.error("No data received from the 'generate_new_data' task.")
            return

        df_to_upload['order_date'] = pd.to_datetime(df_to_upload['order_date'])

        # Filter data for orders within the last 7 days
        df_to_upload = df_to_upload[
            (datetime.now() - df_to_upload['order_date'] >= timedelta(0)) &
            (datetime.now() - df_to_upload['order_date'] <= timedelta(days=7))
        ]

        logging.info(f"{df_to_upload.shape[0]} records received to insert.")
        if df_to_upload.shape[0] != ORDERS_NUMBER:
            logging.error(f"{ORDERS_NUMBER - df_to_upload.shape[0]} were dropped due to date filter.")

        df_to_upload['update_id'] = int(datetime.now().strftime('%Y%m%d%H%M%S'))

        # Insert data into PostgreSQL using the PostgresHook
        try:
            hook = PostgresHook(postgres_conn_id='postgres_1')
            engine = hook.get_sqlalchemy_engine()

            # Use Pandas to insert data into the database
            df_to_upload.to_sql('orders', engine, if_exists='append', index=False)

            logging.info(f"Successfully inserted {df_to_upload.shape[0]} records into the Orders table.")
        except Exception as e:
            logging.error(f"Error during data insertion: {str(e)}")
            raise

    # Task to create the orders table if it doesn't already exist
    create_orders_table_task = PostgresOperator(
        task_id='create_orders_table',
        postgres_conn_id='postgres_1',
        sql=""" CREATE TABLE IF NOT EXISTS Orders (
                order_id uuid NOT NULL PRIMARY KEY,
                customer_email VARCHAR(255) NOT NULL,
                order_date TIMESTAMP NOT NULL,
                amount NUMERIC(10, 2) NOT NULL,
                currency VARCHAR(3) NOT NULL,
                update_id BIGINT NOT NULL
                );""",
    )

    generated_df = generate_new_data()
    inserted = insert_data_into_table(generated_df)

    create_orders_table_task >> generated_df >> inserted

generate_orders()