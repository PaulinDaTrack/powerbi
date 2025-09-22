import os
from dotenv import load_dotenv
load_dotenv()

import requests
import datetime
import mysql.connector
import pytz
import time  # adicionando import time
from authtoken import obter_token

def format_date(date_str):
    if not date_str:
        return None
    try:
        dt = datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return date_str

def to_iso(date_str):
    try:
        dt = datetime.datetime.strptime(date_str, "%d/%m/%Y")
        return dt.strftime("%Y-%m-%dT00:00:00Z")
    except Exception:
        return date_str

def nullify_date(date_str):
    if date_str in ["01/01/1 00:00:00", "01/01/0001 00:00:00"]:
         return None
    return date_str

def processar_grid():
    token = obter_token()
    if not token:
        return

    api_url = "https://integration.systemsatx.com.br/GlobalBus/Grid/List?paramClientIntegrationCode=1003"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    try:
        conn = mysql.connector.connect(
            host=os.getenv("POWERBI_DB_HOST"),
            database=os.getenv("POWERBI_DB_NAME"),
            user=os.getenv("POWERBI_DB_USER"),
            password=os.getenv("POWERBI_DB_PASSWORD")
        )
    except mysql.connector.Error as err:
        print("Erro ao conectar no banco de dados:", err)
        return

    cursor = conn.cursor()
    cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS historico_grades (
        id INT AUTO_INCREMENT PRIMARY KEY,
        line VARCHAR(50),
        estimated_departure VARCHAR(50),
        estimated_arrival VARCHAR(50),
        real_departure VARCHAR(50),
        real_arrival VARCHAR(50),
        route_integration_code VARCHAR(255),
        route_name VARCHAR(255),
        direction_name VARCHAR(255),
        shift VARCHAR(50),
        estimated_vehicle VARCHAR(255),
        real_vehicle VARCHAR(255),
        estimated_distance VARCHAR(50),
        travelled_distance VARCHAR(50),
        client_name VARCHAR(255),
        data_registro DATE,
        UNIQUE KEY idx_codigo_data (route_integration_code, data_registro)
    );
    """)
    conn.commit()

    # Removido update separado; ON DUPLICATE KEY cuidará de atualizar (sem sobrescrever real_* com NULL)
    insert_historico_query = '''
    INSERT INTO historico_grades (
        line, estimated_departure, estimated_arrival, real_departure, real_arrival,
        route_integration_code, route_name, direction_name, shift,
        estimated_vehicle, real_vehicle, estimated_distance, travelled_distance, client_name, data_registro
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        estimated_departure = IF(real_arrival IS NULL OR real_arrival = '' , VALUES(estimated_departure), estimated_departure),
        estimated_arrival = IF(real_arrival IS NULL OR real_arrival = '' , VALUES(estimated_arrival), estimated_arrival),
        real_departure = IF(real_arrival IS NULL OR real_arrival = '' , IFNULL(VALUES(real_departure), real_departure), real_departure),
        real_arrival = IF(real_arrival IS NULL OR real_arrival = '' , IFNULL(VALUES(real_arrival), real_arrival), real_arrival),
        real_vehicle = IF(real_arrival IS NULL OR real_arrival = '' , IFNULL(VALUES(real_vehicle), real_vehicle), real_vehicle),
        estimated_vehicle = IF(real_arrival IS NULL OR real_arrival = '' , VALUES(estimated_vehicle), estimated_vehicle),
        estimated_distance = IF(real_arrival IS NULL OR real_arrival = '' , VALUES(estimated_distance), estimated_distance),
        travelled_distance = IF(real_arrival IS NULL OR real_arrival = '' , VALUES(travelled_distance), travelled_distance),
        route_name = IF(real_arrival IS NULL OR real_arrival = '' , VALUES(route_name), route_name),
        direction_name = IF(real_arrival IS NULL OR real_arrival = '' , VALUES(direction_name), direction_name),
        shift = IF(real_arrival IS NULL OR real_arrival = '' , VALUES(shift), shift),
        client_name = IF(real_arrival IS NULL OR real_arrival = '' , IFNULL(VALUES(client_name), client_name), client_name),
        line = IF(real_arrival IS NULL OR real_arrival = '' , VALUES(line), line)
    '''

    dias_a_verificar = 10
    for i in range(dias_a_verificar):
        data_alvo = datetime.datetime.now(pytz.timezone("America/Sao_Paulo")) - datetime.timedelta(days=i)
        data_formatada = data_alvo.strftime("%d/%m/%Y")
        data_iso = to_iso(data_formatada)

        payload = [{"PropertyName": "EffectiveDate", "Condition": "Equal", "Value": data_iso}]
        response_api = requests.post(api_url, headers=headers, json=payload)

        if response_api.status_code != 200:
            print(f"Erro na API para {data_formatada}: {response_api.status_code}")
            continue

        data = response_api.json()
        if not data:
            print(f"Nenhuma grade encontrada para {data_formatada}")
            continue

        # Pré-filtrar itens não cancelados e coletar códigos
        raw_items = []
        for item in data:
            if item.get('IsTripCanceled') is True:
                continue
            raw_items.append(item)
        if not raw_items:
            print(f"Todas as viagens canceladas em {data_formatada}")
            continue
        route_codes = { (itm.get('RouteIntegrationCode') or '').strip() for itm in raw_items }
        existing_routes = {}
        if route_codes:
            # Montar query IN dinâmica em chunks para evitar limites
            route_codes_list = list(route_codes)
            chunk_size = 1000
            for c in range(0, len(route_codes_list), chunk_size):
                chunk = route_codes_list[c:c+chunk_size]
                placeholders = ','.join(['%s'] * len(chunk))
                cursor.execute(f"SELECT route_integration_code, client_name FROM historico_grades WHERE route_integration_code IN ({placeholders})", chunk)
                for r in cursor.fetchall():
                    existing_routes[r[0]] = r[1]

        batch_data = []
        for item in raw_items:
            line = item.get('LineIntegrationCode')
            estimated_departure = nullify_date(format_date(item.get('EstimatedDepartureDate')))
            estimated_arrival = nullify_date(format_date(item.get('EstimatedArrivalDate')))
            real_departure = nullify_date(format_date(item.get('RealDepartureDate')))
            raw_real_arrival = item.get('RealArrivalDate') or item.get('RealdArrivalDate')
            real_arrival = nullify_date(format_date(raw_real_arrival))
            route_integration_code = (item.get('RouteIntegrationCode') or '').strip()
            route_name = item.get('RouteName')
            direction_name = item.get('DirectionName')
            shift = item.get('Shift')
            estimated_vehicle = item.get('EstimatedVehicle')
            real_vehicle = item.get('RealVehicle')
            estimated_distance = item.get('EstimatedDistance')
            travelled_distance = item.get('TravelledDistance')
            client_name = item.get('ClientName') or existing_routes.get(route_integration_code)
            if client_name:
                client_name = client_name.strip()
            batch_data.append((
                line, estimated_departure, estimated_arrival, real_departure, real_arrival,
                route_integration_code, route_name, direction_name, shift,
                estimated_vehicle, real_vehicle, estimated_distance, travelled_distance,
                client_name, data_alvo.date()
            ))

        # Retry simples para contornar lock wait
        for attempt in range(3):
            try:
                cursor.executemany(insert_historico_query, batch_data)
                conn.commit()
                break
            except mysql.connector.Error as e:
                if e.errno == 1205:  # Lock wait timeout
                    print(f"Lock wait (tentativa {attempt+1}) em {data_formatada}, aguardando...")
                    time.sleep(2 * (attempt + 1))
                    if attempt == 2:
                        raise
                else:
                    raise

        print(f"✅ Grades processadas para {data_formatada}")

    update_travelled_distance_query = """
    UPDATE historico_grades
    SET travelled_distance = FLOOR(estimated_distance)
    WHERE real_arrival IS NOT NULL 
      AND travelled_distance = 0
      AND STR_TO_DATE(estimated_departure, '%d/%m/%Y %H:%i:%s') >= DATE_SUB(NOW(), INTERVAL 7 DAY);
    """
    cursor.execute(update_travelled_distance_query)
    conn.commit()

    cursor.close()
    conn.close()

if __name__ == '__main__':
    processar_grid()
