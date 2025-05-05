from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd

@dag(schedule=None, start_date=datetime(2024, 1, 1), catchup=False, tags=["ejemplo", "ventas"])
def etl_csv_ventas():

    @task
    def extraer():
        df = pd.read_csv("/usr/local/airflow/include/ventas.csv")
        return df.to_dict(orient="records")

    @task
    def transformar(data):
        df = pd.DataFrame(data)
        df["total"] = df["cantidad"] * df["precio_unitario"]
        df["nivel_venta"] = pd.cut(
            df["total"],
            bins=[0, 100, 500, 1000, float("inf")],
            labels=["Bajo", "Medio", "Alto", "Muy Alto"]
        )
        return df.to_dict(orient="records")

    @task
    def cargar(data):
        df = pd.DataFrame(data)
        output_path = "/usr/local/airflow/include/ventas_procesadas.csv"
        df.to_csv(output_path, index=False)
        print("\n✅ Reporte Final de Ventas:")
        print(df[["cliente", "producto", "total", "nivel_venta"]])
        print(f"\n📁 Archivo guardado en: {output_path}")

    # Pipeline ETL
    datos = extraer()
    datos_transformados = transformar(datos)
    cargar(datos_transformados)

etl_csv_ventas()