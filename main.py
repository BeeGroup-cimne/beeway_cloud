import argparse
import time
import uuid
from datetime import datetime
import paho.mqtt.client as mqtt
from dateutil.relativedelta import relativedelta
from paho.mqtt import subscribe
import pandas as pd
from utils.hbase import save_to_hbase
from utils.utils import read_config


def connect_mqtt(client_id, conf):
    def on_connect(_, __, ___, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)
    # Set Connecting Client ID
    c = mqtt.Client(client_id)
    c.username_pw_set(conf["username"], conf["password"])
    c.on_connect = on_connect
    c._connect_timeout = 30
    c.connect(conf["host"], conf["port"])
    return c


def parse_sala(df):
    if "Montcada" not in df["_measurement"].unique():
        return []
    df1 = df.groupby("_measurement")
    df1 = df1.get_group("Montcada")
    if "Sala" not in df1.columns:
        return[]
    df1 = df1[pd.isnull(df1.Sala)==False]
    df1['_time'] = pd.to_datetime(df1['_time']).astype("int") // 10**9
    return df1.drop([x for x in ["result", "table", "_start", "_stop", "Circutor", "_measurement"] if x in df1.columns],
                    axis=1).to_dict(orient="records")


def parse_circutor(df):
    if "Montcada" not in df["_measurement"].unique():
        return []
    df1 = df.groupby("_measurement")
    df1 = df1.get_group("Montcada")
    if "Circutor" not in df1.columns:
        return[]
    df1 = df1[pd.isnull(df1.Circutor) == False]
    df1['_time'] = pd.to_datetime(df1['_time']).astype("int") // 10 ** 9
    return df1.drop([x for x in ["result", "table", "_start", "_stop", "Sala", "_measurement"] if x in df1.columns],
                    axis=1).to_dict(orient="records")


def parse_shelly(df):
    df1 = df.groupby("_measurement")
    tables = [x for x in ['Shelly_H01', 'Shelly_H02', 'Shelly_H03', 'Shelly_H04', 'Shelly_H05', 'Shelly_H06', 'Shelly_T01',
              'Shelly_T02', 'Shelly_T03', 'Shelly_T04', 'Shelly_T05', 'Shelly_T06'] if x in df["_measurement"].unique()]
    if not tables:
        return []
    df1 = pd.concat([x for y, x in df1 if y in tables])
    df1["_field"] = df1['_measurement'].apply(lambda x: x.split("_")[1][0])
    df1["shelly"] = df1['_measurement'].apply(lambda x: x.split("_")[1][1:])
    df1['_time'] = pd.to_datetime(df1['_time']).astype("int") // 10 ** 9
    return df1.drop([x for x in
                     ["result", "table", "_start", "_stop", "Sala", "Circutor", "_measurement"] if x in df1.columns],
                    axis=1).to_dict(orient="records")


def parse_smart(df):
    if "SensorData" not in df["_measurement"].unique():
        return []
    df1 = df.groupby("_measurement")
    df1 = df1.get_group("SensorData")
    if "sensor" not in df1.columns:
        return[]
    df1 = df1[pd.isnull(df1.sensor)==False]
    df1['_time'] = pd.to_datetime(df1['_time']).astype("int") // 10**9
    return df1.drop([x for x in ["result", "table", "_start", "_stop", "_measurement"] if x in df1.columns],
                    axis=1).to_dict(orient="records")


data_types = {
    "montcada": {
        "data": {
            "montcada_sala_beegroup": {
                "parser": parse_sala,
                "row_fields": ["_time", "Sala", "_field"]
            },
            "montcada_circutor_beegroup": {
                "parser": parse_circutor,
                "row_fields": ["_time", "Circutor", "_field"]
            },
            "montcada_shelly_beegroup": {
                "parser": parse_shelly,
                "row_fields": ["_time", "shelly", "_field"]
            }
        },
        "start_date": datetime(2021, 10, 16),
        "topic": "last_time_montcada"
    },
    "smart": {
        "data": {
            "smartdatasystems_beegroup": {
                "parser": parse_smart,
                "row_fields": ["_time", "sensor"]
            }
        },
        "start_date": datetime(2019, 7, 31),
        "topic": "last_time_smart"
    }
}


def store_message(client, userdata, message):
    print("new_msg")
    m = eval(message.payload.decode())
    df = pd.DataFrame.from_records(m)
    last_time = None
    for h_table_name, v in data_types[message.topic]['data'].items():
        documents = v['parser'](df)
        if not documents:
            continue
        last_ts = pd.to_datetime(documents[-1]['_time'], unit="s")
        last_time = last_ts if last_time is None or last_ts > last_time else last_time
        save_to_hbase(documents, h_table_name, config['hbase'], [("info", "all")], row_fields=v["row_fields"])
    client.publish(data_types[message.topic]['topic'], last_time.isoformat(), qos=1, retain=True)
    print("parsed_message")


def get_last_date(client, userdata, message):
    global DATE_START
    DATE_START = datetime.fromisoformat(message.payload.decode())

DATE_START = None
WAIT = 5

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-t", "--type", choices=["sub", "prod"], required=True)
    args = ap.parse_args()
    config = read_config("config.json")
    if args.type == "sub":
        print("starting as consumer")
        cli = connect_mqtt(f"backup_server_{uuid.uuid4()}", config['mqtt'])
        cli.subscribe("backup_data_data")
        cli.subscribe("backup_smart")
        cli.on_message = store_message
        cli.loop_forever()
    elif args.type == "prod":
        print("starting")
        cli_mont = connect_mqtt(f"backup_data_{uuid.uuid4()}", config['mqtt'])
        cli_smart = connect_mqtt(f"backup_data_{uuid.uuid4()}", config['mqtt'])
        cli_mont.subscribe("last_time_montcada")
        cli_smart.subscribe("last_time_smart")
        cli_mont.on_message = get_last_date
        cli_smart.on_message = get_last_date
        wait = WAIT
        while wait:
            cli_mont.loop()
            wait = wait - 1 if wait > 0 else 0
            time.sleep(0.4)
        cli_mont.unsubscribe("last_time_montcada")
        date_start_montcada = DATE_START if DATE_START else data_types['montcada']['start_date']
        DATE_START = None
        wait = WAIT
        while wait:
            cli_smart.loop()
            wait = wait - 1 if wait > 0 else 0
            time.sleep(0.4)
        cli_smart.unsubscribe("last_time_smart")
        date_start_smart = DATE_START if DATE_START else data_types['smart']['start_date']

        cli_mont.publish("make_backup_montcada", date_start_montcada.isoformat(), qos=1)
        cli_smart.publish("make_backup_smart", date_start_smart.isoformat(), qos=1)
