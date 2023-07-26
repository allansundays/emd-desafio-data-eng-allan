import requests
import csv
import pandas as pd
import os
from datetime import datetime, timedelta
from prefect import task, Flow
import requests
import csv
import psycopg2
# import mysql.connector

# Define a URL da API de onde os dados serão obtidos
API_URL = "https://dados.mobilidade.rio/gps/brt"

# Função para capturar os dados da API
@task
def get_data_from_api():
    response = requests.get(API_URL)
    return response.json()

# Função para estruturar os dados recebidos da API
@task
def process_data(data):
    structured_data = []
    for veiculo in data["veiculos"]:
        bus_id = veiculo["codigo"]
        latitude = veiculo["latitude"]
        longitude = veiculo["longitude"]
        speed = veiculo["velocidade"]

        # Cria um dicionário com as informações do ônibus
        bus_info = {
            "bus_id": bus_id,
            "position": f"{latitude}, {longitude}",
            "speed": speed
        }
        structured_data.append(bus_info)
    return structured_data

# Função para salvar os dados em um arquivo CSV
@task
def save_to_csv(data, filename):
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = list(data[0].keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

# Função para carregar dados em uma tabela PostgreSQL
@task
def load_to_db(filename):

    conn = psycopg2.connect(
            host="localhost",
            database="brt",
            user="postgres",
            password="**********",
        )
            

    create_table_query = """
        CREATE TABLE IF NOT EXISTS brt_data (
            id SERIAL PRIMARY KEY,
            bus_id VARCHAR(50),
            position TEXT,
            speed FLOAT
        )
    """

    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
        conn.commit()


    with conn.cursor() as cursor:
        df = pd.read_csv(filename)
        df = df.astype(object).where(pd.notnull(df), None)
        values = list(df.itertuples(index=False, name=None))
        sql = """INSERT INTO brt_data (bus_id, position, speed) VALUES (%s, %s, %s)"""

        cursor.executemany(sql, values)

        conn.commit()
        conn.close()

    # df.to_sql(name="brt_data", con=conn, if_exists="append", index=False)


with Flow("BRT GPS Pipeline") as flow:
    # minutes_to_capture = Parameter("minutes_to_capture", default=10)

    data = get_data_from_api()
    processed_data = process_data(data)
    save_to_csv(processed_data, 'brt_data.csv')
    load_to_db('brt_data.csv')

# Execute a pipeline
flow.run()


