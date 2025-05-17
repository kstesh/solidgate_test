import random
import uuid
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
import pandas as pd
from faker import Faker

fake = Faker()
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
            fake.email(),
            fake.date_time_between(start_date='-7d', end_date='now'),
            round(random.uniform(10, 1000), 2),
            random.choice(currencies)
        )

    # Function to generate a DataFrame of new orders
    def generate_new_data():
        logging.info(f"Generating {ORDERS_NUMBER} orders...")
        orders = [generate_order() for _ in range(ORDERS_NUMBER)]
        logging.info(f"{ORDERS_NUMBER} were generated!")
        return pd.DataFrame(orders, columns=['order_id', 'customer_email', 'order_date', 'amount', 'currency'])

    # Function to insert data into the PostgreSQL table
    def insert_data_into_table(**kwargs):
        logging.info(f"Inserting dato to Orders table...")

        # Get DataFrame from XCom
        df_to_upload = kwargs['ti'].xcom_pull(task_ids='generate_new_data')

        if df_to_upload is None:
            logging.error("No data received from the 'generate_new_data' task.")
            return

        # Filter data for orders within the last 7 days
        df_to_upload = df_to_upload[
            (datetime.now() - df_to_upload['order_date'] >= timedelta(0)) &
            (datetime.now() - df_to_upload['order_date'] <= timedelta(days=7))
        ]
        logging.info(f"{df_to_upload.shape[0]} records received to insert.")
        if df_to_upload.shape[0] != ORDERS_NUMBER:
            logging.error(f"{ORDERS_NUMBER - df_to_upload.shape[0]} were dropped due to date filter.")


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
                currency VARCHAR(3) NOT NULL
                );""",
        dag=dag,
    )

    # Task to generate new data (orders)
    generate_new_data_task = PythonOperator(
        task_id='generate_new_data',
        python_callable=generate_new_data,
        do_xcom_push=True,
        dag=dag,
    )

    # Task to insert the generated data into the PostgreSQL table
    insert_data_into_table_task = PythonOperator(
        task_id='insert_data_into_table',
        python_callable=insert_data_into_table,
        provide_context=True,
        dag=dag,
    )

    create_orders_table_task >> generate_new_data_task >> insert_data_into_table_task