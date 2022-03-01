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
    df1 = df1[pd.isnull(df1.Sala)==False]
    df1['_time'] = pd.to_datetime(df1['_time']).astype("int") // 10**9
    return df1.drop(["result", "table", "_start", "_stop", "Circutor", "_measurement"], axis=1).to_dict(orient="records")


def parse_circutor(df):
    if "Montcada" not in df["_measurement"].unique():
        return []
    df1 = df.groupby("_measurement")
    df1 = df1.get_group("Montcada")
    df1 = df1[pd.isnull(df1.Circutor) == False]
    df1['_time'] = pd.to_datetime(df1['_time']).astype("int") // 10 ** 9
    return df1.drop(["result", "table", "_start", "_stop", "Sala", "_measurement"], axis=1).to_dict(orient="records")


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
    return df1.drop(["result", "table", "_start", "_stop", "Sala", "Circutor", "_measurement"], axis=1).to_dict(orient="records")


data_types = {
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
}


def store_message(client, userdata, message):
    print("new_msg")
    m = eval(message.payload.decode())
    df = pd.DataFrame.from_records(m)
    last_time = None
    for h_table_name, v in data_types.items():
        documents = v['parser'](df)
        if not documents:
            continue
        last_ts = pd.to_datetime(documents[-1]['_time'], unit="s")
        last_time = last_ts if last_time is None or last_ts > last_time else last_time
        save_to_hbase(documents, h_table_name, config['hbase'], [("info", "all")], row_fields=v["row_fields"])
    client.publish("last_time", last_time.isoformat(), qos=1, retain=True)
    print("parsed_message")


def get_last_date(client, userdata, message):
    global WAIT
    global DATE_START
    WAIT = 0
    DATE_START = datetime.fromisoformat(message.payload.decode())


WAIT = 5
DATE_START = datetime(2021, 10, 16)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-t", "--type", choices=["sub", "prod"], required=True)
    args = ap.parse_args()
    config = read_config("config.json")
    if args.type == "sub":
        cli = connect_mqtt("backup_server", config['mqtt'])
        cli.subscribe("backup_data")
        cli.on_message = store_message
        cli.loop_forever()
    elif args.type == "prod":
        print("starting")
        cli = connect_mqtt("backup_data_2", config['mqtt'])
        cli.subscribe("last_time")
        cli.on_message = get_last_date
        while WAIT:
            cli.loop()
            WAIT = WAIT - 1 if WAIT > 0 else 0
            time.sleep(0.4)
        cli.unsubscribe("last_time")
        cli.publish("make_backup", DATE_START.isoformat(), qos=1)
