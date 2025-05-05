from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["demo", "numeros"]
)
def numeros_etl_dag():
    @task
    def extraer():
        # Genera una lista de números del 1 al 10
        return list(range(1, 11))

    @task
    def transformar(numeros):
        # Multiplica cada número por 2
        return [n * 2 for n in numeros]

    @task
    def cargar(numeros_transformados):
        print(f"Números procesados: {numeros_transformados}")

    # Flujo de tareas
    numeros = extraer()
    numeros_transformados = transformar(numeros)
    cargar(numeros_transformados)


# Instanciación del DAG
numeros_etl_dag()