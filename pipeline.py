import requests
import csv
import pandas as pd
import os
from prefect import task, Flow
import requests
import csv
import psycopg2
import subprocess
from time import sleep
import configparser

@task
def get_data_from_api():
    API_URL = "https://dados.mobilidade.rio/gps/brt"
    """ Função para capturar os dados da API """
    response = requests.get(API_URL)
    return response.json()

@task
def process_data(data):
    """ Função para estruturar os dados recebidos da API """
    for veiculo in data["veiculos"]:
        bus_id = veiculo["codigo"]
        placa = veiculo["placa"]
        linha = veiculo["linha"]
        latitude = veiculo["latitude"]
        longitude = veiculo["longitude"]
        data = veiculo["dataHora"]
        speed = veiculo["velocidade"]
        sentido = veiculo["sentido"]
        trajeto = veiculo["trajeto"]
        hodometro = veiculo["hodometro"]
        direcao = veiculo["direcao"]


        # Cria um dicionário com as informações do ônibus
        bus_info = {
            "bus_id": bus_id,
            "placa": placa,
            "linha": linha,
            "position": f"{latitude}, {longitude}",
            "data": data,
            "speed": speed,
            "sentido": sentido,
            "trajeto": trajeto,
            "hodometro": hodometro,
            "direcao": direcao
        }
        structured_data.append(bus_info)
    return structured_data

@task
def save_to_csv(data, filename):
    """ Função para salvar os dados em um arquivo CSV """
    with open(filename, 'w', newline='', encoding='latin1') as csvfile:
        fieldnames = list(data[0].keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


@task
def load_to_db(filename):

    config = configparser.ConfigParser()
    config.read("config.ini")
    """ Função para carregar dados em uma tabela PostgreSQL """
    conn = psycopg2.connect(
            host=config.get("postgresql", "host"),
            database=config.get("postgresql", "database"),
            user= config.get("postgresql", "user"),
            password=config.get("postgresql", "password"),
            port=config.get("postgresql", "port")
        )

    def convert_to_datetime(data_str):
        return pd.to_datetime(data_str)    

    create_table_query = """
        CREATE TABLE IF NOT EXISTS brt_data (
            id SERIAL PRIMARY KEY,
            bus_id VARCHAR(50),
            license_plate VARCHAR(50),
            bus_line VARCHAR(50),
            position TEXT,
            data TIMESTAMP,
            speed FLOAT,
            flow TEXT,
            path TEXT,
            km FLOAT,
            direction TEXT) """

    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
        conn.commit()


    with conn.cursor() as cursor:
        df = pd.read_csv('brt_data.csv', encoding='latin1')
        df['data'] = df.apply(lambda row: convert_to_datetime(row["data"]), axis=1)
        df = df.astype(object).where(pd.notnull(df), None)
        values = list(df.itertuples(index=False, name=None))
        sql = """INSERT INTO brt_data (bus_id, license_plate, bus_line,
        position, data, speed,
        flow, path, km, direction) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        cursor.executemany(sql, values)

        conn.commit()
        conn.close()



@task
def run_dbt_modeling():
    current_directory = os.path.abspath(os.path.dirname(__file__))
    project = 'brt_dbt'
    path = f"{current_directory}\{project}"

    try:
        # Muda para o diretório do projeto dbt
        os.chdir(path)
        # Executa o comando dbt run usando subprocess
        subprocess.run("dbt run", shell=True, check=True)
        print("Modelagem dbt concluída com sucesso!")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar o comando dbt: {e}")

with Flow("BRT GPS Pipeline") as flow:
    structured_data = []
    for _ in range(10):
        data = get_data_from_api()
        processed_data = process_data(data)
        sleep(60)
    save_to_csv(structured_data, 'brt_data.csv')
    load_task = load_to_db('brt_data.csv')
    run_task = run_dbt_modeling()
    run_task.set_upstream(load_task)

flow.run()