"""
## Astronaut ETL example DAG

This DAG queries the list of astronauts currently in space from the
Open Notify API and prints each astronaut's name and flying craft.

There are two tasks, one to get the data from the API and save the results,
and another to print the results. Both tasks are written in Python using
Airflow's TaskFlow API, which allows you to easily turn Python functions into
Airflow tasks, and automatically infer dependencies and pass data.

The second task uses dynamic task mapping to create a copy of the task for
each Astronaut in the list retrieved from the API. This list will change
depending on how many Astronauts are in space, and the DAG will adjust
accordingly each time it runs.

For more explanation and getting started instructions, see our Write your
first DAG tutorial: https://www.astronomer.io/docs/learn/get-started-with-airflow

![Picture of the ISS](https://www.esa.int/var/esa/storage/images/esa_multimedia/images/2010/02/space_station_over_earth/10293696-3-eng-GB/Space_Station_over_Earth_card_full.jpg)
"""

from airflow.sdk.definitions.asset import Asset # ASSET SE USA PARA REGISTRAR LA SALIDA COMO UN DATO RASTREABLE
from airflow.decorators import dag, task # DAG Y TASK PERMITEN USAR LA TASKFLOW API (MUCHO MÁS LEGIBLE)
from pendulum import datetime # PENDULUM ES LA LIBRERÍA RECOMENDADA POR AIRFLOW PARA FECHAS
import requests


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1), # INICIO
    schedule="@daily", # FRECUENCIA
    catchup=False, # DESACTIVADO PARA QUE NO SE RECALCULEN EJECUCIONES ANTIGUAS
    doc_md=__doc__, # DOCSTRING: VISIBLE EN LA INTERFAZ COMO DOCUMENTACIÓN DEL DAG
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def example_astronauts():
    # Define tasks
    @task( # TAREA 1: OBTENER ASTRONAUTAS
        # Define a dataset outlet for the task. This can be used to schedule downstream DAGs when this task has run.
        outlets=[Asset("current_astronauts")]
    )  # Define that this task updates the `current_astronauts` Dataset
    def get_astronauts(**context) -> list[dict]:
        """
        This task uses the requests library to retrieve a list of Astronauts
        currently in space. The results are pushed to XCom with a specific key
        so they can be used in a downstream pipeline. The task returns a list
        of Astronauts to be used in the next task.
        """
        try:
            r = requests.get("http://api.open-notify.org/astros.json") # HACE UNA SOLICITUD HTTP A OPEN NOTIFY API
            r.raise_for_status()
            number_of_people_in_space = r.json()["number"]
            list_of_people_in_space = r.json()["people"]
        except Exception:
            print("API currently not available, using hardcoded data instead.") # SI FALLA HACE UN FALLBACK CON UNA LISTA FIJA
            number_of_people_in_space = 12
            list_of_people_in_space = [
                {"craft": "ISS", "name": "Oleg Kononenko"},
                {"craft": "ISS", "name": "Nikolai Chub"},
                {"craft": "ISS", "name": "Tracy Caldwell Dyson"},
                {"craft": "ISS", "name": "Matthew Dominick"},
                {"craft": "ISS", "name": "Michael Barratt"},
                {"craft": "ISS", "name": "Jeanette Epps"},
                {"craft": "ISS", "name": "Alexander Grebenkin"},
                {"craft": "ISS", "name": "Butch Wilmore"},
                {"craft": "ISS", "name": "Sunita Williams"},
                {"craft": "Tiangong", "name": "Li Guangsu"},
                {"craft": "Tiangong", "name": "Li Cong"},
                {"craft": "Tiangong", "name": "Ye Guangfu"},
            ]

        context["ti"].xcom_push(
            key="number_of_people_in_space", value=number_of_people_in_space
        )
        return list_of_people_in_space # DEVUELVE UNA LISTA DE DICCIONARIOS CON ASTRONAUTAS

    @task # TAREA 2: MOSTRAR LOS ASTRONAUTAS CON DYNAMIC TASK MAPPING
    def print_astronaut_craft(greeting: str, person_in_space: dict) -> None:
        """
        This task creates a print statement with the name of an
        Astronaut in space and the craft they are flying on from
        the API request results of the previous task, along with a
        greeting which is hard-coded in this example.
        """
        craft = person_in_space["craft"]
        name = person_in_space["name"]

        print(f"{name} is currently in space flying on the {craft}! {greeting}") # MUESTRA UN SALUDO PERSONALIZADO Y EL VEHÍCULO ESPACIAL DE CADA ASTRONAUTA

    # Use dynamic task mapping to run the print_astronaut_craft task for each
    # Astronaut in space
    print_astronaut_craft.partial(greeting="Hello! :)").expand( # AIRFLOW GENERA AUTOMÁTICAMENTE UNA TAREA POR CADA ÍTEM RETORNADO
        person_in_space=get_astronauts()  # Define dependencies using TaskFlow API syntax
    )


# Instantiate the DAG
example_astronauts()
