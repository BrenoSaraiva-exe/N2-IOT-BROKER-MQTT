import csv
import json

import boto3
import paho.mqtt.client as mqtt

EMPLOYEES = []
BROKER = '13.218.123.35'

sns = boto3.resource('sns', region_name='sa-east-1')
PHONE_NUMBER = "+5511972762209"

def alert_security(name: str):
    message = f"Alert! Non-employee individual identified: {name}"
    sns.meta.client.publish(PhoneNumber=PHONE_NUMBER, Message=message)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker")
        client.subscribe('camera/entrada')
    else:
        print("Failed to connect, return code %d\n", rc)

def on_message(client, userdata, msg):
    try:
        msg_data = json.loads(msg.payload.decode('utf-8'))
        print("Received:", msg_data)
        if not any(e['name'] == msg_data.get("name") for e in EMPLOYEES):
            alert_security(msg_data.get('name'))
    except Exception as e:
        print("Error processing message:", e)

def connect():
    conn = mqtt.Client()
    conn.on_connect = on_connect
    conn.on_message = on_message
    conn.connect(BROKER, 1883, 60)
    return conn

if __name__ == "__main__":
    with open('employees.csv', 'r') as fr:
        reader = csv.DictReader(fr)
        EMPLOYEES = list(reader)

    conn = connect()
    conn.loop_forever()
