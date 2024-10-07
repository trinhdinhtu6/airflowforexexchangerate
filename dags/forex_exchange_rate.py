import json
import os
from datetime import datetime

import numpy as np
import pandas as pd
import pendulum
import requests
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

input_path = "/opt/airflow/data/input"

output_path = "/opt/airflow/data/output"


def _store_data(ti):
    data = ti.xcom_pull(task_ids="get_api")
    data_dict = json.loads(data)
    data_lst = []

    if not os.path.exists(input_path):
        os.makedirs(input_path)

    input_file = os.path.join(input_path, "input6.json")
    # If the json file exist
    if os.path.exists(input_file):
        # Read the json file
        with open(input_file, "r") as f:
            try:
                # Load existing JSON data
                data_lst = json.load(f)
            except json.JSONDecodeError:
                # If the file is empty or invalid,
                # initialize with an empty list or dict
                data_lst = []
    else:
        data_lst = []

    data_lst.append(data_dict)

    with open(input_file, "w") as f:
        json.dump(data_lst, f, indent=4)

    print("Save successfully!")
    with open(input_file, "r") as f:
        latest_data = json.load(f)
    print(f"Data after the execution: {json.dumps(latest_data, indent=4)}")
    ti.xcom_push(key="new_data", value=data_dict)


def _insert_data(ti):
    pg = PostgresHook(postgres_conn_id="postgres_conn")
    conn = pg.get_conn()
    cursor = conn.cursor()

    # Insert data
    insert_query = """
        INSERT INTO forex_exchange_rates_base_usd (base, date, eur,
        usd, nzd, gbp, jpy, cad, vnd)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (base, date)
        DO UPDATE
            SET eur = excluded.eur,
            usd = excluded.usd,
            nzd = excluded.nzd,
            gbp = excluded.gbp,
            jpy = excluded.jpy,
            cad = excluded.cad,
            vnd = excluded.vnd;
    """
    data = ti.xcom_pull(task_ids="store_data", key="new_data")
    rates = data["rates"]
    insert_new_data = (
        data["base"],
        data["date"],
        rates["EUR"],
        rates["USD"],
        rates["NZD"],
        rates["GBP"],
        rates["JPY"],
        rates["CAD"],
        rates["VND"],
    )

    cursor.execute(insert_query, insert_new_data)
    conn.commit()

    # Thực hiện query để lấy dữ liệu từ bảng
    cursor.execute(
        "SELECT * FROM forex_exchange_rates_base_usd;"
    )  # Lấy 10 dòng dữ liệu đầu tiên từ bảng
    records = cursor.fetchall()  # Lấy tất cả các kết quả

    # In kết quả ra màn hình
    for row in records:
        print(f"Row: {row}")

    # Close
    cursor.close()
    conn.close()


def cal_max_amplitude_symbol(row, df, symbol):
    # Absolute of the subdivide between symbol of this row to other
    diff = df[symbol].sub(row[symbol]).abs()
    # Delete the current row
    diff[df["date"] == row["date"]] = -np.inf

    # Get the maximum value of the amplitudes in this symbol
    max_amplitude = diff.max()
    max_date = df["date"][diff.idxmax()]
    return max_amplitude, max_date


def _execute_and_load_pandas(ti, **context):
    pg = PostgresHook(postgres_conn_id="postgres_conn")
    conn = pg.get_conn()
    cursor = conn.cursor()

    select_query = """
        SELECT * FROM forex_exchange_rates_base_usd;
    """

    cursor.execute(select_query)
    records = cursor.fetchall()

    # In kết quả ra màn hình
    print("----- The Current Table -----")
    for row in records:
        print(f"Row: {row}")

    columns = [column[0] for column in cursor.description]
    df = pd.DataFrame(records, columns=columns)
    num_rows, num_cols = df.shape
    print(f"Number of Row: {num_rows}")
    if num_rows <= 1:
        print("Not enough data!")
    else:
        symbols = ["eur", "usd", "nzd", "gbp", "jpy", "cad", "vnd"]
        current_date = context["execution_date"].date()
        current_date_row = df[df["date"] == current_date].iloc[0]
        result = []
        for symbol in symbols:
            max_amplitude, max_date = cal_max_amplitude_symbol(
                current_date_row, df, symbol
            )
            result.append(
                {
                    "run_date": current_date_row["date"],
                    "max_amplitude_date": max_date,
                    "symbol": symbol,
                    "max_amplitude": max_amplitude,
                }
            )
        result_df = pd.DataFrame(result)

        print(result_df)

        if not os.path.exists(output_path):
            os.makedirs(output_path)
        file_name = f"/max_amplitude_base_usd_{current_date}.csv"
        load_csv_path = output_path + file_name
        result_df.to_csv(
            load_csv_path,
            header=True,
        )
        ti.xcom_push(key="csv_path", value=load_csv_path)


def _send_telegram(ti):
    api_token = Variable.get("API_TOKEN")
    channel_id = Variable.get("CHANNEL_ID")
    # Prepare the data
    data = {
        "chat_id": channel_id,
        "caption": "Here is your CSV file",
    }
    file_path = ti.xcom_pull(task_ids="execute_and_load_pandas",
                             key="csv_path")
    # Open the file and send the request
    if file_path is None:
        print("""
              Because there arent any csv file that has been created
              at the last task. So there will be no file to send at this task
              """)
        # ỦL to send the message
        url = f"https://api.telegram.org/bot{api_token}/sendMessage"
        data = {
            "chat_id": channel_id,
            "text": "Sorry. No data yet:("
        }
        response = requests.post(url, data=data)
    else:
        # URL to send the file
        url = f"https://api.telegram.org/bot{api_token}/sendDocument"
        with open(file_path, "rb") as file:
            files = {"document": file}
            response = requests.post(url, data=data, files=files)

    # Check response status
    if response.status_code == 200:
        print("File sent successfully")
    else:
        print(f"Failed to send file. Error: {response.status_code}")


with DAG(
    dag_id="forex_change_rate",
    start_date=datetime(2024, 9, 1, tzinfo=pendulum.timezone("Asia/Jakarta")),
    schedule_interval="0 1 * * *",
    catchup=True,
) as dag:
    # Lấy API_KEY từ biến môi trường
    api_key = os.getenv("FOREX_API_KEY") # GET YOUR OWN API KEY

    # Phần endpoint cũng bị dài quá không xuống dòng được anh ơi:((
    get_api = SimpleHttpOperator(
        task_id="get_api",
        http_conn_id="forex_apilayer_conn",
        endpoint='exchangerates_data/{{(execution_date).strftime("%Y-%m-%d")}}?symbols=&base=USD',
        method="GET",
        headers={"apikey": f"{ api_key }"},
    )

    store_data = PythonOperator(
        task_id="store_data", python_callable=_store_data, provide_context=True
    )

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS forex_exchange_rates_base_usd (
                id SERIAL PRIMARY KEY,
                base CHAR(3),
                date DATE NOT NULL,
                eur FLOAT,
                usd FLOAT,
                nzd FLOAT,
                gbp FLOAT,
                jpy FLOAT,
                cad FLOAT,
                vnd FLOAT
            );

            CREATE UNIQUE INDEX IF NOT EXISTS unique_forex_exchange_rates_base_usd_base_date
            ON forex_exchange_rates_base_usd (base, date);
        """,
    )

    insert_data = PythonOperator(
        task_id="insert_data",
        python_callable=_insert_data,
        provide_context=True
    )

    execute_and_load_pandas = PythonOperator(
        task_id="execute_and_load_pandas",
        python_callable=_execute_and_load_pandas
    )

    send_telegram = PythonOperator(
        task_id="send_telegram",
        python_callable=_send_telegram
    )


get_api >> [store_data, create_table] >> insert_data
insert_data >> execute_and_load_pandas >> send_telegram
