import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

default_args = {
    'owner': 'hwanseung2',
    'depends_on_past': False,  # 이전 DAG의 Task가 성공, 실패 여부에 따라 현재 DAG 실행 여부가 결정. False는 과거의 실행 결과 상관없이 매일 실행한다
    'start_date': days_ago(2), #datetime(2023, 1, 20),
    'retires': 5,
    'retry_delay': timedelta(minutes=5)  # 만약 실패하면 5분 뒤 재실행
}

with DAG(dag_id = 'etlPipeline', default_args = default_args, schedule_interval = '30 * * * *', tags = ['etltesting']) as dag:
    task0 = BashOperator(
        task_id = 'pwd',
        bash_command = "pwd"
    )
    
    task1 = BashOperator(
        task_id = 'ChangeDirectory',
        bash_command = "cd /opt/ml/github/RecommendU-etl/crawling"
    )

    task2 = BashOperator(
        task_id = 'CrawlingLink',
        bash_command = "python crawl_link.py"
    )

    task3 = BashOperator(
        task_id = 'CrawlingCoverLetter',
        bash_command = "python extract.py"
    )

    task0 >> task1
    task1 >> task2
    task2 >> task3