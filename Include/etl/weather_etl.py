import requests
import psycopg2

# -------------------------------
# CONFIG
# -------------------------------
LATITUDE = 13.0827
LONGITUDE = 80.2707

DB_CONFIG = {
    "host": "postgres",
    "database": "postgres",
    "user": "postgres",
    "password": "postgres",
    "port": "5432"
}

# -------------------------------
# EXTRACT
# -------------------------------
def extract_weather_data():
    url = f"https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        raise Exception(f"API Error: {e}")

# -------------------------------
# TRANSFORM
# -------------------------------
def transform_weather_data(data):
    current = data.get("current_weather", {})
    
    transformed = {
        "latitude": data.get("latitude"),
        "longitude": data.get("longitude"),
        "temperature": current.get("temperature"),
        "windspeed": current.get("windspeed"),
        "winddirection": current.get("winddirection")
    }
    
    return transformed

# -------------------------------
# LOAD
# -------------------------------
def load_weather_data(data):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Insert data
    cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection)
        VALUES (%s, %s, %s, %s, %s);
    """, (
        data["latitude"],
        data["longitude"],
        data["temperature"],
        data["windspeed"],
        data["winddirection"]
    ))

    conn.commit()
    cursor.close()
    conn.close()

# -------------------------------
# MAIN FUNCTION
# -------------------------------
def run_etl():
    data = extract_weather_data()
    transformed = transform_weather_data(data)
    load_weather_data(transformed)