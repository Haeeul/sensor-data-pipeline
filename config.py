import os

from dotenv import load_dotenv

load_dotenv()

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "sensors")
MQTT_QOS = 1  # at-least-once: 유실 방지, 중복은 DB 유니크 제약으로 처리

MSSQL_HOST = os.getenv("MSSQL_HOST", "localhost")
MSSQL_PORT = int(os.getenv("MSSQL_PORT", "1433"))
MSSQL_USER = os.getenv("MSSQL_USER", "sa")
MSSQL_PASSWORD = os.getenv("MSSQL_SA_PASSWORD", "Sensor!Pass2026")
MSSQL_DB = os.getenv("MSSQL_DB", "sensordb")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_INTERVAL_SEC = float(os.getenv("FLUSH_INTERVAL_SEC", "0.5"))
