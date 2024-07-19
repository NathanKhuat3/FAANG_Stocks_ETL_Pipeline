from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

faang_companies = ['META', 'AAPL', 'AMZN', 'NFLX', 'GOOG']

def transform_load_data(task_instance):

    for comp in faang_companies:
    
        globals()[f"data_{comp}"] = task_instance.xcom_pull(task_ids=f"group_a.group_b.tsk_extract_{comp}_data")

        values = list(globals()[f"data_{comp}"]['Monthly Time Series'].values())

        ticker = globals()[f"data_{comp}"]['Meta Data']['2. Symbol']
        date = list(globals()[f"data_{comp}"]['Monthly Time Series'].keys())
        open_values = [float(value['1. open']) for value in values]
        high_values = [float(value['2. high']) for value in values]
        low_values = [float(value['3. low']) for value in values]
        close_values = [float(value['4. close']) for value in values]
        volume_values = [float(value['5. volume']) for value in values]

        globals()[f"all_data_{comp}"] = {
        'Ticker': ticker,
        'date': date,
        'open': open_values,
        'high': high_values,
        'low': low_values,
        'close': close_values,
        'volume': volume_values
        }

        globals()[f"df_{comp}"] = pd.DataFrame(globals()[f"all_data_{comp}"])

        globals()[f"df_{comp}"]['date'] = pd.to_datetime(globals()[f"df_{comp}"]['date'])
        globals()[f"df_{comp}"]['date'] = globals()[f"df_{comp}"]['date'].dt.strftime('%Y-%m')
    
    all_df = [df_META, df_AMZN, df_AAPL, df_NFLX, df_GOOG]
    combined_df = pd.concat(all_df, ignore_index=True)

    combined_df.to_csv("faang_data.csv", index=False, header=False)

def load_data():
    hook = PostgresHook(postgres_conn_id= 'postgres_conn')
    hook.copy_expert(
        sql= "COPY faang_historic_data FROM stdin WITH DELIMITER as ','",
        filename='faang_data.csv'
    )

def save_joined_data_s3(task_instance):
    data = task_instance.xcom_pull(task_ids="task_join_data")
    df = pd.DataFrame(data, columns = ['Ticker', 'date', 'open', 'high', 'low', 'close', 'volume','Stock_exchange', 'IPO_Date', 'Headquarters', 'CEO', 'Products_and_Services', 'Primary_Competitors', 'Significant_Historical_Events'])
    df.to_csv("s3://faang-prj-bucket/output_faang_data.csv", index=False)



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG('faang_dag_1',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:

        start_pipeline = DummyOperator(
            task_id = 'tsk_start_pipeline'
        )

        join_data = PostgresOperator(
                task_id='task_join_data',
                postgres_conn_id = "postgres_conn",
                sql= '''SELECT 
                    fh.Ticker,                    
                    date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    Stock_exchange,
                    IPO_Date,
                    Headquarters,
                    CEO,
                    Products_and_Services,
                    Primary_Competitors,
                    Significant_Historical_Events
                    FROM faang_historic_data fh
                    INNER JOIN faang_comp_data fc
                        ON fh.Ticker = fc.Ticker                                     
                ;
                '''
            )

        load_joined_data = PythonOperator(
            task_id= 'task_load_joined_data',
            python_callable=save_joined_data_s3
            )

        end_pipeline = DummyOperator(
                task_id = 'task_end_pipeline'
        )

        with TaskGroup(group_id = 'group_a', tooltip= "Extract_from_S3_and_API") as group_A:
            
            create_table_1 = PostgresOperator(
                task_id='tsk_create_table_1',
                postgres_conn_id = "postgres_conn",
                sql= '''  
                    CREATE TABLE IF NOT EXISTS faang_comp_data (
                    Ticker TEXT NOT NULL,
                    Stock_exchange TEXT NOT NULL,
                    IPO_Date TEXT NOT NULL,
                    Headquarters TEXT NOT NULL,
                    CEO TEXT NOT NULL,
                    Products_and_Services TEXT NOT NULL,
                    Primary_Competitors TEXT NOT NULL,
                    Significant_Historical_Events TEXT NOT NULL              
                );
                '''
            )

            truncate_table = PostgresOperator(
                task_id='tsk_truncate_table',
                postgres_conn_id = "postgres_conn",
                sql= ''' TRUNCATE TABLE faang_comp_data;
                    '''
            )

            uploadS3_to_postgres  = PostgresOperator(
                task_id = "tsk_uploadS3_to_postgres",
                postgres_conn_id = "postgres_conn",
                sql = "SELECT aws_s3.table_import_from_s3('faang_comp_data', '', '(format csv, DELIMITER '','', HEADER true)', 'faang-prj-bucket', 'FAANG_static_data.csv', 'us-east-2');"
            )

            create_table_2 = PostgresOperator(
                task_id='tsk_create_table_2',
                postgres_conn_id = "postgres_conn",
                sql= '''  
                    CREATE TABLE IF NOT EXISTS faang_historic_data (
                    Ticker TEXT NOT NULL,
                    Date TEXT NOT NULL,
                    Open NUMERIC,
                    High NUMERIC,
                    Close NUMERIC,
                    Low NUMERIC,
                    Volume NUMERIC          
                );
                '''
            )


            with TaskGroup(group_id = 'group_b', tooltip= "Extract_companies") as group_B:

                for comp in faang_companies:
                    
                    globals()[f"is_api_ready_{comp}"] = HttpSensor(
                        task_id =f'tsk_is_api_ready_{comp}',
                        http_conn_id='alpha_avantage_api',
                        endpoint=f'https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol={comp}&apikey=demo'
                    )

                    globals()[f"extract_{comp}_data"] = SimpleHttpOperator(
                        task_id = f'tsk_extract_{comp}_data',
                        http_conn_id = 'alpha_avantage_api',
                        endpoint=f'https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol={comp}&apikey=demo',
                        method = 'GET',
                        response_filter= lambda r: json.loads(r.text),
                        log_response=True
                    )
                
                is_api_ready_META >> extract_META_data
                is_api_ready_AMZN >> extract_AMZN_data
                is_api_ready_AAPL >> extract_AAPL_data
                is_api_ready_NFLX >> extract_NFLX_data
                is_api_ready_GOOG >> extract_GOOG_data

            transform_faang_data = PythonOperator(
                task_id= 'transform_load_faang_data',
                python_callable=transform_load_data
            )

            load_faang_data = PythonOperator(
            task_id= 'tsk_load_faang_data',
            python_callable=load_data
            )

            create_table_1 >> truncate_table >> uploadS3_to_postgres
            create_table_2 >> group_B >> transform_faang_data >> load_faang_data
        start_pipeline >> group_A >> join_data >> load_joined_data >> end_pipeline