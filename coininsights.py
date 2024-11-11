import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator,S3DeleteObjectsOperator, S3ListOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner':'Srujan',
    'start_date' : days_ago(1)
}

dag = DAG(
    dag_id = 'coininsights-pipeline',
    default_args = default_args,
    schedule_interval = None,
    catchup = False
)

data_extract = LambdaInvokeFunctionOperator(
    task_id = 'data_extract',
    function_name = 'cryptoinsight-etl-data-extract',
    aws_conn_id = 'aws_conn',
    region_name = 'us-east-1',
    dag=dag
)

check_for_raw_data_task = S3KeySensor(
    task_id = 'check_for_raw_data',
    aws_conn_id = 'aws_conn',
    bucket_name = 'cryptoinsight-etl-project-bucket',
    bucket_key = 'raw_data/',
    timeout = 60*60, #1hour
    poke_interval = 60,
    mode = 'reschedule',
    dag = dag
)

raw_data_transform_task = LambdaInvokeFunctionOperator(
    task_id = 'raw_data_transform',
    function_name = 'cryptoinsight-etl-data-transform',
    aws_conn_id = 'aws_conn',
    region_name = 'us-east-1',
    dag=dag
)


check_for_coin_info_file_task = S3KeySensor(
    task_id = 'check_for_coin_info_file',
    aws_conn_id = 'aws_conn',
    bucket_name = 'cryptoinsight-etl-project-bucket',
    bucket_key = 'transformed_data/coin_info/',
    poke_interval = 60,
    timeout = 60*60,
    mode = 'reschedule',
    dag = dag
)

check_for_coin_metrics_file_task = S3KeySensor(
    task_id = 'check_for_coin_metrics_file',
    aws_conn_id = 'aws_conn',
    bucket_name = 'cryptoinsight-etl-project-bucket',
    bucket_key = 'transformed_data/coin_metrics/',
    poke_interval = 60,
    timeout = 60*60,
    mode = 'reschedule',
    dag = dag
)

load_to_coin_info_stage_table_task = RedshiftDataOperator(
    task_id = 'load_to_coin_info_stage_table',
    # redshift_conn_id = 'redshift_conn',
    aws_conn_id = 'aws_conn',
    database = 'dev',
    workgroup_name = 'coin-insights',  # Your Redshift Serverless workgroup name
    sql = """
    COPY dev.public.coin_info_stage (id, name, symbol, image_url) 
    FROM 's3://cryptoinsight-etl-project-bucket/transformed_data/coin_info' 
    IAM_ROLE 'arn:aws:iam::396913696969:role/service-role/AmazonRedshift-CommandsAccessRole-20241110T170722' 
    FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'us-east-1'
    """,
    wait_for_completion = True,  # Important for Redshift Serverless to ensure query completes
    region_name = 'us-east-1',
    dag = dag
)


load_to_coin_metrics_stage_table_task = RedshiftDataOperator(
    task_id = 'load_to_coin_metrics_stage_table',
    # redshift_conn_id = 'redshift_conn',
    aws_conn_id = 'aws_conn',
    database = 'dev',
    workgroup_name = 'coin-insights',  # Your Redshift Serverless workgroup name
    sql = """
    COPY dev.public.coin_metrics_stage (id, current_price_usd, market_cap, market_cap_rank, total_volume, price_change_percentage_24h, market_cap_change_percentage_24h, high_24h, low_24h, price_change_24h, circulating_supply, total_supply, max_supply, last_updated) 
    FROM 's3://cryptoinsight-etl-project-bucket/transformed_data/coin_metrics' 
    IAM_ROLE 'arn:aws:iam::396913696969:role/service-role/AmazonRedshift-CommandsAccessRole-20241110T170722' 
    FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'us-east-1'
    """,
    wait_for_completion = True,  # Important for Redshift Serverless to ensure query completes
    region_name = 'us-east-1',
    dag = dag
)

move_to_coin_info_target_table_task = RedshiftDataOperator(
    task_id = "move_to_coin_info_target_table",
    aws_conn_id = 'aws_conn',
    database = 'dev',
    workgroup_name = 'coin-insights',
    sql = """
    MERGE INTO dev.public.dim_coin_info
    USING dev.public.coin_info_stage
    ON dim_coin_info.id = coin_info_stage.id
    WHEN MATCHED THEN
        UPDATE SET
            name = coin_info_stage.name,
            symbol = coin_info_stage.symbol,
            image_url = coin_info_stage.image_url
    WHEN NOT MATCHED THEN
        INSERT (id, name, symbol, image_url)
        VALUES (coin_info_stage.id, coin_info_stage.name, coin_info_stage.symbol, coin_info_stage.image_url)
    """,
    wait_for_completion = True,  # Important for Redshift Serverless to ensure query completes
    region_name = 'us-east-1',
    dag = dag
)


move_to_coin_metrics_target_table_task = RedshiftDataOperator(
    task_id = "move_to_coin_metrics_target_table",
    aws_conn_id = 'aws_conn',
    database = 'dev',
    workgroup_name = 'coin-insights',
    sql = """
    MERGE INTO dev.public.fact_coin_metrics
    USING dev.public.coin_metrics_stage
    ON fact_coin_metrics.id = coin_metrics_stage.id
    WHEN MATCHED THEN
        UPDATE SET
            current_price_usd=coin_metrics_stage.current_price_usd, 
            market_cap=coin_metrics_stage.market_cap, 
            market_cap_rank=coin_metrics_stage.market_cap_rank, 
            total_volume=coin_metrics_stage.total_volume, 
            price_change_percentage_24h=coin_metrics_stage.price_change_percentage_24h, 
            market_cap_change_percentage_24h=coin_metrics_stage.market_cap_change_percentage_24h, 
            high_24h=coin_metrics_stage.high_24h, 
            low_24h=coin_metrics_stage.low_24h, 
            price_change_24h=coin_metrics_stage.price_change_24h, 
            circulating_supply=coin_metrics_stage.circulating_supply, 
            total_supply=coin_metrics_stage.total_supply, 
            max_supply=coin_metrics_stage.max_supply, 
            last_updated=coin_metrics_stage.last_updated
    WHEN NOT MATCHED THEN
        INSERT (id, current_price_usd, market_cap, market_cap_rank, total_volume, price_change_percentage_24h, market_cap_change_percentage_24h, high_24h, low_24h, price_change_24h, circulating_supply, total_supply, max_supply, last_updated)
        VALUES (coin_metrics_stage.id, coin_metrics_stage.current_price_usd, coin_metrics_stage.market_cap, coin_metrics_stage.market_cap_rank, coin_metrics_stage.total_volume, coin_metrics_stage.price_change_percentage_24h, coin_metrics_stage.market_cap_change_percentage_24h, coin_metrics_stage.high_24h, coin_metrics_stage.low_24h, coin_metrics_stage.price_change_24h, coin_metrics_stage.circulating_supply, coin_metrics_stage.total_supply, coin_metrics_stage.max_supply, coin_metrics_stage.last_updated)
    """,
    wait_for_completion = True,  # Important for Redshift Serverless to ensure query completes
    region_name = 'us-east-1',
    dag = dag
)


truncate_coin_info_stage_table_task = RedshiftDataOperator(
    task_id='truncate_coin_info_stage_table',
    aws_conn_id='aws_conn',
    database='dev',
    workgroup_name='coin-insights',
    sql="TRUNCATE TABLE dev.public.coin_info_stage;",
    wait_for_completion=True,
    region='us-east-1',
    dag=dag
)


truncate_coin_metrics_stage_table_task = RedshiftDataOperator(
    task_id='truncate_coin_metrics_stage_table',
    aws_conn_id='aws_conn',
    database='dev',
    workgroup_name='coin-insights',
    sql="TRUNCATE TABLE dev.public.coin_metrics_stage;",
    wait_for_completion=True,
    region='us-east-1',
    dag=dag
)


# archiving and deletring
get_raw_files_task = S3ListOperator(
    task_id = 'get_raw_files',
    aws_conn_id = 'aws_conn',
    bucket = 'cryptoinsight-etl-project-bucket',
    prefix = 'raw_data/',
    dag=dag 
)


get_coin_info_files_task = S3ListOperator(
    task_id = 'get_coin_info_files',
    aws_conn_id = 'aws_conn',
    bucket = 'cryptoinsight-etl-project-bucket',
    prefix = 'transformed_data/coin_info/',
    dag=dag 
)

get_coin_metrics_files_task = S3ListOperator(
    task_id = 'get_coin_metrics_files',
    aws_conn_id = 'aws_conn',
    bucket = 'cryptoinsight-etl-project-bucket',
    prefix = 'transformed_data/coin_metrics/',
    dag=dag
)


def copy_raw_files_to_archive(**kwargs):
    ti = kwargs['ti']
    file_keys = ti.xcom_pull(task_ids='get_raw_files')
    for file_key in file_keys:
        copy_task = S3CopyObjectOperator(
            task_id = f"copy_{file_key.replace('/', '_').replace(' ', '_').replace(':', '_')}",
            aws_conn_id = 'aws_conn',
            source_bucket_name='cryptoinsight-etl-project-bucket',
            source_bucket_key = file_key,
            dest_bucket_name = 'cryptoinsight-etl-project-bucket',
            dest_bucket_key = f'archive/{file_key}',
            dag=dag
        )
        copy_task.execute(context=kwargs)



def copy_coin_info_files_to_archive(**kwargs):
    ti = kwargs['ti']
    file_keys = ti.xcom_pull(task_ids='get_coin_info_files')
    for file_key in file_keys:
        copy_task = S3CopyObjectOperator(
            task_id = f"copy_{file_key.replace('/', '_').replace(' ', '_').replace(':', '_')}",
            aws_conn_id = 'aws_conn',
            source_bucket_name='cryptoinsight-etl-project-bucket',
            source_bucket_key = file_key,
            dest_bucket_name = 'cryptoinsight-etl-project-bucket',
            dest_bucket_key = f'archive/{file_key}',
            dag=dag
        )
        copy_task.execute(context=kwargs)


def copy_coin_metrics_files_to_archive(**kwargs):
    ti = kwargs['ti']
    file_keys = ti.xcom_pull(task_ids='get_coin_metrics_files')
    for file_key in file_keys:
        copy_task = S3CopyObjectOperator(
            task_id = f"copy_{file_key.replace('/', '_').replace(' ', '_').replace(':', '_')}",
            aws_conn_id = 'aws_conn',
            source_bucket_name='cryptoinsight-etl-project-bucket',
            source_bucket_key = file_key,
            dest_bucket_name = 'cryptoinsight-etl-project-bucket',
            dest_bucket_key = f'archive/{file_key}',
            dag=dag
        )
        copy_task.execute(context=kwargs)
        
        
def delete_raw_files_from_source(**kwargs):
    ti = kwargs['ti']
    file_keys = ti.xcom_pull(task_ids='get_raw_files')
    for file_key in file_keys:
        delete_task = S3DeleteObjectsOperator(
            task_id = f"delete_{file_key.replace('/', '_').replace(' ', '_').replace(':', '_')}",
            aws_conn_id = 'aws_conn',
            bucket = 'cryptoinsight-etl-project-bucket',
            keys = file_key,
            dag=dag
        )
        
        delete_task.execute(context=kwargs)
        


def delete_coin_info_files_from_source(**kwargs):
    ti = kwargs['ti']
    file_keys = ti.xcom_pull(task_ids='get_coin_info_files')
    for file_key in file_keys:
        delete_task = S3DeleteObjectsOperator(
            task_id = f"delete_{file_key.replace('/', '_').replace(' ', '_').replace(':', '_')}",
            aws_conn_id = 'aws_conn',
            bucket = 'cryptoinsight-etl-project-bucket',
            keys = file_key,
            dag=dag
        )
        
        delete_task.execute(context=kwargs)
        
        
        
def delete_coin_metrics_files_from_source(**kwargs):
    ti = kwargs['ti']
    file_keys = ti.xcom_pull(task_ids='get_coin_metrics_files')
    for file_key in file_keys:
        delete_task = S3DeleteObjectsOperator(
            task_id = f"delete_{file_key.replace('/', '_').replace(' ', '_').replace(':', '_')}",
            aws_conn_id = 'aws_conn',
            bucket = 'cryptoinsight-etl-project-bucket',
            keys = file_key,
            dag=dag
        )

        delete_task.execute(context=kwargs)
        


archive_raw_files_task = PythonOperator(
    task_id = 'archive_raw_files',
    python_callable = copy_raw_files_to_archive,
    dag=dag
)


archive_coin_info_files_task = PythonOperator(
    task_id = 'archive_coin_info_files',
    python_callable = copy_coin_info_files_to_archive,
    dag=dag
)


archive_coin_metrics_files_task = PythonOperator(
    task_id = 'archive_coin_metrics_files',
    python_callable = copy_coin_metrics_files_to_archive,
    dag=dag
)


delete_raw_files_task = PythonOperator(
    task_id = 'delete_raw_files',
    python_callable = delete_raw_files_from_source,
    dag=dag
)

delete_coin_info_files_task = PythonOperator(
    task_id = 'delete_coin_info_files',
    python_callable = delete_coin_info_files_from_source,
    dag=dag
)

delete_coin_metrics_files_task = PythonOperator(
    task_id = 'delete_coin_metrics_files',
    python_callable = delete_coin_metrics_files_from_source,
    dag=dag
)




data_extract >> check_for_raw_data_task 
check_for_raw_data_task >> [raw_data_transform_task, get_raw_files_task]
raw_data_transform_task >>[check_for_coin_info_file_task, check_for_coin_metrics_file_task]
check_for_coin_info_file_task >> load_to_coin_info_stage_table_task >> move_to_coin_info_target_table_task >> truncate_coin_info_stage_table_task >> get_coin_info_files_task >> archive_coin_info_files_task >> delete_coin_info_files_task
check_for_coin_metrics_file_task >> load_to_coin_metrics_stage_table_task >> move_to_coin_metrics_target_table_task >> truncate_coin_metrics_stage_table_task >> get_coin_metrics_files_task >> archive_coin_metrics_files_task >> delete_coin_metrics_files_task
[delete_coin_info_files_task, delete_coin_metrics_files_task, get_raw_files_task] >> archive_raw_files_task >> delete_raw_files_task 