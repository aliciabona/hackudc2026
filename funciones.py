import requests
import time
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Conf influxdb

URL_INFLUX = "http://127.0.0.1:8086"
TOKEN = "jo2VDPyQTSqTOyXkdhvHBsajtoGhnHfSMcj82cGr4bn32EJj0LrPh1UkNoVAd6DW_QiSCBLe6-74pcO2AxXekg=="
ORG = "elb"
BUCKET = "jellyfish"

# Playas y coordenadas

PLAYAS = {
    "1":  {"nombre": "Patos (Nigrán)", "lat": 42.170, "lon": -8.910},
    "2":  {"nombre": "A Lanzada (O Grove)", "lat": 42.460, "lon": -8.970},
    "3":  {"nombre": "Illa de Arousa", "lat": 42.590, "lon": -8.960},
    "4":  {"nombre": "Lariño (Muros)", "lat": 42.790, "lon": -9.200},
    "5":  {"nombre": "Area da Vila (Camariñas)", "lat": 43.150, "lon": -9.280},
    "6":  {"nombre": "Razo (Carballo)", "lat": 43.330, "lon": -8.780},
    "7":  {"nombre": "Riazor (A Coruña)", "lat": 43.420, "lon": -8.480},
    "8":  {"nombre": "Doniños (Ferrol)", "lat": 43.550, "lon": -8.400},
    "9":  {"nombre": "Pantín (Valdoviño)", "lat": 43.680, "lon": -8.190},
    "10": {"nombre": "Fornos (Cariño)", "lat": 43.790, "lon": -7.960},
    "11": {"nombre": "Areoura (Foz)", "lat": 43.720, "lon": -7.440},
    "12": {"nombre": "As Catedrais (Ribadeo)", "lat": 43.600, "lon": -7.180}
}

# Nivel Plancton

def calcular_plancton(temp_agua):
    nivel = 0.2 + (temp_agua - 12) * 0.08
    return round(max(0.1, min(1.0, nivel)), 2)

# Riesgo Calaveras Portuguesas

def calcular_riesgo(temp_agua, viento, olas, plancton):
    riesgo = (
        (temp_agua * 0.18) +
        (plancton * 10 * 0.22) +
        (max(0, 6 - viento) * 0.12) +
        (max(0, 2 - olas) * 0.12)
    )
    return round(min(10, riesgo), 2)

# Obtener datos Open Meteo

def obtener_datos_openmeteo(lat, lon):

    # Clima
    url_clima = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}"
        f"&current=wind_speed_10m,wind_direction_10m,weather_code"
        f"&wind_speed_unit=ms"
    )

    # Datos marinos
    url_marina = (
        f"https://marine-api.open-meteo.com/v1/marine?"
        f"latitude={lat}&longitude={lon}"
        f"&hourly=sea_surface_temperature,wave_height"
    )

    res_clima = requests.get(url_clima, timeout=10)
    res_clima.raise_for_status()
    clima_json = res_clima.json()

    res_marina = requests.get(url_marina, timeout=10)
    res_marina.raise_for_status()
    marina_json = res_marina.json()

    temp_agua = marina_json["hourly"]["sea_surface_temperature"][-1]
    olas = marina_json["hourly"]["wave_height"][-1]

    return clima_json["current"], temp_agua, olas

# Sistema

def monitor_jellyfish():

    client = InfluxDBClient(url=URL_INFLUX, token=TOKEN, org=ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    try:
        while True:

            print("\n--- MONITOREO MEDUSAS GALICIA ---\n")
            for key, playa in PLAYAS.items():
                print(f"{key}. {playa['nombre']}")

            opcion = input("\nSeleccione playa (0 salir): ")

            if opcion == "0":
                break
            if opcion not in PLAYAS:
                print("Seleccion no valida.")
                continue

            p = PLAYAS[opcion]
            print(f"\nConsultando {p['nombre']}...")

            try:
                clima, temp_agua, olas = obtener_datos_openmeteo(p["lat"], p["lon"])

                viento = float(clima["wind_speed_10m"])
                direccion = float(clima["wind_direction_10m"])
                codigo_clima = int(clima["weather_code"])

                plancton = calcular_plancton(temp_agua)
                riesgo = calcular_riesgo(temp_agua, viento, olas, plancton)

                punto = (
                    Point("peligrosidad_medusas")
                    .tag("ubicacion", p["nombre"])
                    .field("temp_agua", float(temp_agua))
                    .field("viento_vel", viento)
                    .field("viento_dir", direccion)
                    .field("altura_olas", float(olas))
                    .field("nivel_plancton", plancton)
                    .field("indice_riesgo", riesgo)
                    .field("clima_cod", codigo_clima)
                )

                write_api.write(bucket=BUCKET, record=punto)

                print(f"Agua: {temp_agua}°C | Olas: {olas} m")
                print(f"Viento: {viento} m/s ({direccion}°)")
                print(f"Plancton: {int(plancton*100)}%")
                print(f"Indice riesgo: {riesgo}/10")
                print(f"Codigo clima: {codigo_clima}")

            except Exception as e:
                print("Error tecnico:", e)

            time.sleep(1)

    finally:
        client.close()
        print("Conexion Influx cerrada.")