# =========================================================
# IMPORTS
# =========================================================

import json
import logging
import math
import os
from datetime import datetime

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================================================
# ENVIRONNEMENT FLINK
# =========================================================

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# =========================================================
# CHARGEMENT DU JAR  — méthode fiable en mode local
# =========================================================

# Modifie ce chemin si nécessaire (ou définis la variable FLINK_JAR_PATH)
JAR_PATH = os.environ.get(
    "FLINK_JAR_PATH",
    "file:///C:/Users/Abdou/Documents/Projet%20PI-SPI/flink_lib/flink-connector-kafka-3.0.2-1.18.jar"
)
# IMPORTANT : les espaces dans le chemin doivent être encodés %20
# Exemple : "Projet PI-SPI" → "Projet%20PI-SPI"

env.add_jars(JAR_PATH)

# =========================================================
# KAFKA CONSUMER
# =========================================================

properties = {
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "group.id":          os.environ.get("KAFKA_GROUP_ID", "flink-consumer"),
    "auto.offset.reset": "latest",
}

consumer = FlinkKafkaConsumer(
    topics=os.environ.get("KAFKA_TOPIC", "pispi_transactions"),
    deserialization_schema=SimpleStringSchema(),
    properties=properties,
)

ds = env.add_source(consumer)

# =========================================================
# FEATURE ENGINEERING
# =========================================================

def process(tx_json):
    try:
        tx        = json.loads(tx_json)
        amount    = float(tx.get("amount", 0))
        timestamp = int(tx.get("timestamp", 0))

        if timestamp > 0:
            dt   = datetime.fromtimestamp(timestamp / 1000)
            hour = dt.hour
        else:
            hour = 0

        return json.dumps({
            "payer_alias":     tx.get("payer_alias"),
            "event_timestamp": timestamp,
            "amount":          amount,
            "log_amount":      math.log1p(amount),
            "hour":            hour,
            "is_night_time":   1 if hour < 6 else 0,
        })

    except Exception as e:
        logger.warning("Enregistrement ignoré : %s", e)
        return None

# =========================================================
# APPLY + FILTER + OUTPUT
# =========================================================

processed = ds.map(process).filter(lambda x: x is not None)
processed.print()

# =========================================================
# EXECUTE
# =========================================================

env.execute("PI-SPI Flink Feature Stream")