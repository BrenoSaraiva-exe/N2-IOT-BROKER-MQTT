import paho.mqtt.client as mqtt
import json
import time
import random
import csv

BROKER_ADDRESS = "13.218.123.35"
BROKER_PORT = 1883
TOPIC = "camera/entrada"
CSV_FILE = "byPassers.csv"

client = mqtt.Client("camera_simulada")

def connect():
    try:
        client.connect(BROKER_ADDRESS, BROKER_PORT, 60)
        print("Conectado ao broker MQTT")
    except Exception as e:
        print("Erro ao conectar:", e)

def load_people_from_csv(filepath):
    people = []
    try:
        with open(filepath, mode='r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                people.append(row)
        print(f"Carregado dados de {len(people)} pessoas do arquivo {filepath}")
    except FileNotFoundError:
        print(f"Erro: Arquivo CSV '{filepath}' não encontrado.")
    except Exception as e:
        print(f"Erro ao ler o arquivo CSV: {e}")
    return people

def simulate_people_passing(people_data):
    if not people_data:
        print("Nenhum dado de pessoa para simular. Encerrando.")
        return
    while True:
        person_payload = random.choice(people_data).copy()
        person_payload["timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S')

        payload = json.dumps(person_payload)
        client.publish(TOPIC, payload)
        print(f"Enviado: {payload}")
        time.sleep(random.randint(2, 5))

if __name__ == "__main__":
    people_data = load_people_from_csv(CSV_FILE)
    if people_data:
        connect()
        simulate_people_passing(people_data)
