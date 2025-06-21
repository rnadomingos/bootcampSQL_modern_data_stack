from time import sleep
from airflow.decorators import dag, task
from datetime import datetime

@dag(
        dag_id="My first DAG",
        description="First DAG with air flow",
        schedule="40 9 * * *",
        start_date=datetime(2024,6,21),
        catchup=False #backfill
)
def pipeline():

    @task
    def primeira_atividade():
        print("primeira atividade rodou com sucesso")
        sleep(2)
    
    @task
    def segunda_atividade():
        print("segunda atividade rodou com sucesso")
        sleep(2)
    
    @task
    def terceira_atividade():
        print("terceira atividade rodou com sucesso")
        sleep(2)

    t1 = primeira_atividade()
    t2 = segunda_atividade()
    t3 = terceira_atividade()

pipeline()    