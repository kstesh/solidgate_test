import numpy as np
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from sqlalchemy import create_engine
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import logging
import pandas as pd
import os

EXCHANGE_API_KEY = os.getenv('OPEN_EXCHANGE_RATES_APP_ID')
POSTGRES_1_CONN_ID = "postgres_1"
POSTGRES_2_CONN_ID = "postgres_2"
DEFAULT_CURRENCY = "EUR"

@dag(
    dag_id='transfer_orders',
    description="Transfer data from POSTGRES1.ORDERS to POSTGRES2.ORDERS_EUR",
    schedule="@hourly",
    start_date=datetime(2025, 5, 16),
    catchup=False,
)
def transfer_orders():

    create_orders_eur_table_task = PostgresOperator(
        task_id='create_orders_eur_table',
        postgres_conn_id=POSTGRES_2_CONN_ID,
        sql=""" CREATE TABLE IF NOT EXISTS Orders_eur (
                order_id uuid NOT NULL PRIMARY KEY,
                customer_email VARCHAR(255) NOT NULL,
                order_date TIMESTAMP NOT NULL,
                amount NUMERIC(10, 2) NOT NULL,
                currency VARCHAR(3) NOT NULL,
                update_id BIGINT NOT NULL
                );""",
    )

    @task
    def get_exchange_rates() -> pd.DataFrame:
        logging.info("Fetching exchange rates from API.")
        url = f"https://openexchangerates.org/api/latest.json?app_id={EXCHANGE_API_KEY}"

        try:
            response = requests.get(url)

            if response.status_code != 200:
                logging.error(f"API request failed with status {response.status_code}: {response.text}")
                raise AirflowFailException(f"API request failed: {response.status_code}")

            data = response.json()
            rates = data['rates']

            rates_df = pd.DataFrame(list(rates.items()), columns=['currency', 'rate'])
            rates_df['rate'] = pd.to_numeric(rates_df['rate'], errors='raise', downcast='float')

            logging.info(f"Exchange rates successfully fetched for {rates_df.shape[0]} currencies.")

            return rates_df

        except requests.exceptions.RequestException as e:
            logging.error(f"Network error during API request: {e}")
            raise AirflowFailException("Network error")

    @task
    def get_last_update_id() -> int:
        logging.info("Fetching last update_id from the Orders_eur table.")

        try:
            hook = PostgresHook(postgres_conn_id=POSTGRES_2_CONN_ID)
            sql = "SELECT MAX(update_id) FROM orders_eur"
            result = hook.get_first(sql)

            last_update_id = result[0] if result and result[0] is not None else None

            logging.info(f"Max update_id in Orders_eur = {last_update_id}")

            return last_update_id
        except Exception as e:
            logging.error(f"Error during fetching last update_id: {str(e)}")
            raise

    @task
    def fetch_new_orders(last_update_id: int)-> pd.DataFrame:
        logging.info("Fetching new orders from Orders table.")

        try:
            hook = PostgresHook(postgres_conn_id=POSTGRES_1_CONN_ID)

            if last_update_id is None:
                logging.warning("No last_update_id provided. Fetching all orders.")

                sql = "SELECT * FROM orders"
                records = hook.get_pandas_df(sql)

                logging.info(f"Fetched {records.shape[0]} new orders.")

                return records
            else:
                sql = "SELECT * FROM orders WHERE update_id > %(last_update_id)s"
                records = hook.get_pandas_df(sql, parameters={"last_update_id": last_update_id})

                logging.info(f"Fetched {records.shape[0]} new orders.")

                return records
        except Exception as e:
            logging.error(f"Failed to fetch new orders: {str(e)}")
            raise

    @task
    def convert_currency(orders_df: pd.DataFrame, rates_df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Converting currency for new orders.")

        merged_df = pd.merge(orders_df, rates_df, on='currency', how='left')
        merged_df.rename(columns={'amount': 'amount_orig'}, inplace=True)

        if merged_df['rate'].isnull().any():
            missing_currencies = merged_df[merged_df['rate'].isnull()]['currency'].unique()
            logging.error(f"Unknown rates for currencies: {', '.join(missing_currencies)}")
            raise

        merged_df['amount'] = np.round(merged_df['amount_orig'] / merged_df['rate'], 2)
        merged_df['currency'] = 'EUR'

        merged_df = merged_df[['order_id', 'customer_email', 'order_date', 'amount', 'currency', 'update_id']]

        logging.info(f"Converted {merged_df.shape[0]} orders to EUR.")
        logging.info(f"Convertion check: \n{merged_df[['amount', 'currency']].head(3)}")

        return merged_df

    @task
    def insert_orders_eur(df_to_upload: pd.DataFrame):
        logging.info(f"Inserting {df_to_upload.shape[0]} records into Orders_eur table.")

        try:
            hook = PostgresHook(postgres_conn_id=POSTGRES_2_CONN_ID)

            #kostyl but works:) connection approach should be reviewed
            conn_str = hook.get_uri()
            logging.info(f"URL {conn_str}")

            # remove __extra__ parameter
            cleaned_conn_str = conn_str.replace('?__extra__=%7B%7D', '')

            engine = create_engine(cleaned_conn_str)
            df_to_upload.to_sql('orders_eur', engine, if_exists='append', index=False)

            logging.info(f"Successfully inserted {df_to_upload.shape[0]} records into the Orders_eur table.")
        except Exception as e:
            logging.error(f"Error during data insertion: {str(e)}")
            raise

    table = create_orders_eur_table_task

    # Use `.set_downstream()` explicitly for operator -> task
    exchange_rates = get_exchange_rates()
    last_update_id = get_last_update_id()

    # downstream manually
    create_orders_eur_table_task.set_downstream([exchange_rates, last_update_id])

    new_orders = fetch_new_orders(last_update_id)
    converted_orders = convert_currency(new_orders, exchange_rates)
    insert_orders_eur(converted_orders)

transfer_orders()