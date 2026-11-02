import os
import threading
import time
from datetime import datetime, timezone
from flask import Flask, jsonify, render_template
import requests
import psycopg2
import paho.mqtt.client as mqtt

app = Flask(__name__)

ultima_posicion = {
    "latitude": 0.0,
    "longitude": 0.0,
    "altitude": 0.0,
    "velocity": 0.0,
    "visibility": "unknown",
    "timestamp": 0
}

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = 1883
DB_HOST = os.getenv("DB_HOST", "postgres_iss_db")
DB_NAME = os.getenv("DB_NAME", "postgres_iss")
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "root")

TOPIC_LAT = "ieslavereda/iss/latitude"
TOPIC_LON = "ieslavereda/iss/longitude"
TOPIC_ALT = "ieslavereda/iss/altitude"
TOPIC_VEL = "ieslavereda/iss/velocity"

API_ISS = "https://api.wheretheiss.at/v1/satellites/25544"

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def guardar_log(nivel, mensaje):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO log_sistema (nivel, mensaje) VALUES (%s, %s)", (nivel, mensaje)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error guardando log en PostgreSQL: {e}", flush=True)

def guardar_telemetria_db(data):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        timestamp_dt = datetime.fromtimestamp(data["timestamp"], tz=timezone.utc)
        cursor.execute(
            "INSERT INTO telemetria_iss (timestamp_api, latitude, longitude, altitude, velocity, visibility, footprint, solar_lat, solar_lon) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (
                timestamp_dt,
                float(data["latitude"]),
                float(data["longitude"]),
                float(data["altitude"]),
                float(data["velocity"]),
                str(data["visibility"]),
                float(data["footprint"]),
                float(data.get("solar_lat", 0.0)),
                float(data.get("solar_lon", 0.0))
            )
        )

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error al guardar en telemetria_iss: {e}", flush=True)

def actualizar_estadisticas_diarias(vel, alt):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # SQL UPSERT: Inserta o actualiza los máximos, mínimos y medias automáticamente
        query = """
        INSERT INTO estadisticas_diarias (
            fecha, velocidad_max, velocidad_min, velocidad_media, 
            altitud_max, altitud_min, total_lecturas, ultima_actualizacion
        ) VALUES (
            CURRENT_DATE, %s, %s, %s, %s, %s, 1, CURRENT_TIMESTAMP
        )
        ON CONFLICT (fecha) DO UPDATE SET
            velocidad_max = GREATEST(estadisticas_diarias.velocidad_max, EXCLUDED.velocidad_max),
            velocidad_min = LEAST(estadisticas_diarias.velocidad_min, EXCLUDED.velocidad_min),
            velocidad_media = (estadisticas_diarias.velocidad_media * estadisticas_diarias.total_lecturas + EXCLUDED.velocidad_media) / (estadisticas_diarias.total_lecturas + 1),
            altitud_max = GREATEST(estadisticas_diarias.altitud_max, EXCLUDED.altitud_max),
            altitud_min = LEAST(estadisticas_diarias.altitud_min, EXCLUDED.altitud_min),
            total_lecturas = estadisticas_diarias.total_lecturas + 1,
            ultima_actualizacion = CURRENT_TIMESTAMP;
        """
        cursor.execute(query, (vel, vel, vel, alt, alt))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error actualizando estadisticas_diarias: {e}", flush=True)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api/iss/actual')
def iss_actual():
    return jsonify(ultima_posicion)

@app.route('/api/iss/historial')
def iss_historial():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT timestamp_api, latitude, longitude, altitude, velocity, visibility FROM telemetria_iss ORDER BY timestamp_api DESC LIMIT 10"
        )
        filas = cursor.fetchall()
        cursor.close()
        conn.close()
        resultado = []
        for f in filas:
            resultado.append({
                "timestamp": f[0].isoformat(),
                "latitude": f[1],
                "longitude": f[2],
                "altitude": f[3],
                "velocity": f[4],
                "visibility": f[5]
            })
        return jsonify(resultado)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def bucle_captura_iss():
    global ultima_posicion

    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    except AttributeError:
        client = mqtt.Client()

    conectado = False
    while not conectado:
        try:
            print(f"Conectando a MQTT en {MQTT_HOST}:{MQTT_PORT}...", flush=True)
            client.connect(MQTT_HOST, MQTT_PORT, 60)
            conectado = True
            print("¡Conexión a MQTT establecida!", flush=True)
            guardar_log("INFO", "Contenedor Flask conectado con éxito a Mosquitto.")
        except Exception as e:
            print(f"Esperando a Mosquitto ({e})... Reintentando en 3s", flush=True)
            time.sleep(3)

    headers = {"User-Agent": "Mozilla/5.0"}

    while True:
        try:
            response = requests.get(API_ISS, headers=headers, timeout=5)
            if response.status_code == 200:
                data = response.json()
                
                lat = float(data["latitude"])
                lon = float(data["longitude"])
                alt = float(data["altitude"])
                vel = float(data["velocity"])

                ultima_posicion = {
                    "latitude": lat,
                    "longitude": lon,
                    "altitude": alt,
                    "velocity": vel,
                    "visibility": data.get("visibility", "unknown"),
                    "timestamp": time.time()
                }

                # Publicación de datos reales de la ISS en los topics de MQTT
                client.publish(TOPIC_LAT, lat)
                client.publish(TOPIC_LON, lon)
                client.publish(TOPIC_ALT, alt)
                client.publish(TOPIC_VEL, vel)

                # 1. Guardar telemetría continua en PostgreSQL
                guardar_telemetria_db(data)

                # 2. Actualizar estadísticas diarias (máximos, mínimos y promedios)
                actualizar_estadisticas_diarias(vel, alt)

                print(f"[ISS - IES La Vereda] Lat: {lat:.4f} | Lon: {lon:.4f} | Alt: {alt:.2f} km | Vel: {vel:.0f} km/h", flush=True)
        except Exception as e:
            err_msg = f"Error en la llamada a la API ISS: {e}"
            print(err_msg, flush=True)
            guardar_log("ERROR", err_msg)

        time.sleep(2)

if __name__ == '__main__':
    hilo = threading.Thread(target=bucle_captura_iss, daemon=True)
    hilo.start()

    app.run(host='0.0.0.0', port=5000)