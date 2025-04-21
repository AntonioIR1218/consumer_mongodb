from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient
from datetime import datetime
from flask import Flask, jsonify
import threading
import logging
import json
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

app = Flask(__name__)

# Configuración actualizada de Kafka
KAFKA_CONFIG = {
    'bootstrap.servers': 'd035kvrb92dfgde406p0.any.us-east-1.mpx.prd.cloud.redpanda.com:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'sasl.username': 'tony',
    'sasl.password': '1234',
    'group.id': 'music-consumer-group',  # Actualizado el group.id
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': 'false'
}

# Configuración de MongoDB (debes reemplazar con tus credenciales reales)
MONGO_URI = "mongodb+srv://tony:1234@people.rjn70.mongodb.net/?retryWrites=true&w=majority&appName=people"
DB_NAME = "musica"
COLLECTION_NAME = "ExplicitEnergyTracks"  # Nombre más descriptivo
TOPIC = "explicit_energy_tracks"  # Tópico actualizado

def get_mongo_collection():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME][COLLECTION_NAME], client

def parse_dates(data: dict):
    """Parsea las fechas en los datos del track"""
    try:
        if 'metadata' in data and 'imported_at' in data['metadata']:
            date_str = data['metadata']['imported_at']
            if isinstance(date_str, str):
                data['metadata']['imported_at'] = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f')
    except Exception as e:
        logging.warning(f"Error al parsear fechas: {e}")

def enrich_track_data(data):
    """Añade campos calculados a los datos del track"""
    try:
        # Calcular duración en minutos
        data['duration_min'] = round(data.get('duration_ms', 0) / 60000, 2)
        
        # Clasificar energía
        energy = data.get('energy', 0)
        if energy > 0.8:
            data['energy_level'] = 'high'
        elif energy > 0.5:
            data['energy_level'] = 'medium'
        else:
            data['energy_level'] = 'low'
            
        return data
    except Exception as e:
        logging.warning(f"Error enriqueciendo datos: {e}")
        return data

def insert_track(data):
    try:
        collection, client = get_mongo_collection()
        parse_dates(data)
        enriched_data = enrich_track_data(data)

        result = collection.update_one(
            {'_id': enriched_data['_id']},
            {'$set': enriched_data},
            upsert=True
        )
        client.close()

        if result.upserted_id:
            logging.info(f"Track insertado: ID {enriched_data['_id']} - {enriched_data.get('name')}")
        elif result.modified_count > 0:
            logging.info(f"Track actualizado: ID {enriched_data['_id']} - {enriched_data.get('name')}")
        else:
            logging.info(f"Track no modificado: ID {enriched_data['_id']}")
        return True

    except Exception as e:
        logging.error(f"Error al insertar en MongoDB: {e}")
        return False

def kafka_consumer_loop():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])
    logging.info(f"Consumer iniciado. Suscrito al tópico: {TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                logging.error(f"Error al consumir: {msg.error()}")
                break

            try:
                logging.info(f"Mensaje recibido. Offset: {msg.offset()}")
                data = json.loads(msg.value().decode('utf-8'))
                logging.debug(f"Datos del track: {data.get('name')}")

                if insert_track(data):
                    consumer.commit(asynchronous=False)

            except json.JSONDecodeError as e:
                logging.warning(f"Error JSON: {e} - Mensaje: {msg.value()}")
            except Exception as e:
                logging.error(f"Error procesando mensaje: {e}")

    except KeyboardInterrupt:
        logging.info("Consumer detenido por teclado.")
    except Exception as e:
        logging.error(f"Error inesperado: {e}")
    finally:
        consumer.close()
        logging.info("Consumer cerrado.")
        
@app.route("/health")
def health_check():
    """Endpoint de healthcheck mejorado"""
    try:
        # Verificar conexión a MongoDB
        _, client = get_mongo_collection()
        client.server_info()
        client.close()
        return jsonify({"status": "healthy", "services": ["kafka", "mongodb"]}), 200
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500

def main():
    threading.Thread(target=kafka_consumer_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)

if __name__ == '__main__':
    main()
