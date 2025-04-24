from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import random
import datetime as dt
import psycopg2
from pymongo.mongo_client import MongoClient
from models.heat_and_humidity import HeatAndHumidityMeasureEvent

# MongoDB bağlantısı
client = MongoClient("mongodb+srv://cetingokhan:cetingokhan@cluster0.ff5aw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")

USERNAME = "hafsaduman"

def generate_random_heat_and_humidity_data(dummy_record_count: int):
    records = []
    for _ in range(dummy_record_count):
        temperature = random.randint(10, 40)
        humidity = random.randint(10, 100)
        timestamp = dt.datetime.now()
        creator = "airflow"
        record = HeatAndHumidityMeasureEvent(temperature, humidity, timestamp, creator)
        records.append(record)
    return records

def create_sample_data_on_mongodb():
    records = generate_random_heat_and_humidity_data(10)
    db = client["bigdata_training"]
    collection = db[f"user_coll_{USERNAME}"]
    for record in records:
        collection.insert_one(record.__dict__)
        
def copy_anomalies_into_new_collection():
    db = client["bigdata_training"]
    source = db[f"user_coll_{USERNAME}"]
    target = db[f"anomalies_{USERNAME}"]
    anomalies = source.find({"temperature": {"$gt": 30}})
    for anomaly in anomalies:
        anomaly.pop("_id", None)  # _id varsa sil
        anomaly["creator"] = USERNAME
        target.insert_one(anomaly)


def copy_airflow_logs_into_new_collection():
    conn = psycopg2.connect(
        host="postgres",
        port="5432",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()
    one_minute_ago = datetime.now() - timedelta(minutes=1)
    cursor.execute("""
        SELECT event, COUNT(*) 
        FROM log 
        WHERE dttm >= %s 
        GROUP BY event
    """, (one_minute_ago,))
    logs = cursor.fetchall()

    db = client["bigdata_training"]
    log_coll = db[f"logs_{USERNAME}"]

    for log in logs:
        event_name, count = log
        log_coll.insert_one({
            "event_name": event_name,
            "record_count": count,
            "created_at": datetime.now()
        })

    cursor.close()
    conn.close()

with DAG(
    dag_id="homework",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    schedule_interval="0 0 * * *"
) as dag:

    start = DummyOperator(task_id="start")

    create_sample_data = PythonOperator(
        task_id="create_sample_data",
        python_callable=create_sample_data_on_mongodb
    )

    copy_anomalies = PythonOperator(
        task_id="copy_anomalies_into_new_collection",
        python_callable=copy_anomalies_into_new_collection
    )

    insert_logs = PythonOperator(
        task_id="insert_airflow_logs_into_mongodb",
        python_callable=copy_airflow_logs_into_new_collection
    )

    final = DummyOperator(task_id="finaltask")

    # DAG task flow (flow.png'e uygun olarak)
    start >> create_sample_data >> copy_anomalies >> final
    start >> insert_logs >> final
