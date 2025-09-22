import os
from dotenv import load_dotenv
load_dotenv()

import requests
import mysql.connector
from datetime import datetime
from authtoken import obter_token
import time
from dateutil import parser
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
import atexit

def routeviolation(token):
    parana_tz = pytz.timezone("America/Sao_Paulo")
    hoje = datetime.now(parana_tz).date()

    url = "https://integration.systemsatx.com.br/GlobalBus/Trip/TripsWithNonConformity"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        conn = mysql.connector.connect(
            host=os.getenv("POWERBI_DB_HOST"),
            database=os.getenv("POWERBI_DB_NAME"),
            user=os.getenv("POWERBI_DB_USER"),
            password=os.getenv("POWERBI_DB_PASSWORD")
        )
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS informacoes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                LineName VARCHAR(255),
                RouteName VARCHAR(255),
                Direction VARCHAR(255),
                RealVehicle VARCHAR(255),
                url VARCHAR(512),
                data_execucao DATE,
                violation_type VARCHAR(255),
                UNIQUE (RouteName, data_execucao)
            )
        """)

        for coluna in ['url', 'violation_type']:
            try:
                cursor.execute(f"ALTER TABLE informacoes ADD COLUMN {coluna} VARCHAR(512)")
            except mysql.connector.Error as err:
                if err.errno != 1060:
                    raise

        initial_date = f"{hoje}T00:00:00.000Z"
        final_date = f"{hoje}T23:59:59.999Z"

        payload = {
            "ClientIntegrationCode": "1003",
            "InitialDate": initial_date,
            "FinalDate": final_date,
            "DelayTolerance": 5,
            "EarlinessTolerance": 5,
            "InconformityType": 1
        }

        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()

        print(f"📅 Processando violações de {hoje}...")

        if not data:
            print("🔸 Nenhuma violação encontrada.")
        else:
            if isinstance(data, dict):
                data = [data]

            insert_data = []

            for item in data:
                route_name = item.get("RouteName")
                if not route_name:
                    continue

                original_url = item.get("URL", "")
                url = original_url.replace("bus.systemsatx.com.br", "http://educacaorumocerto.trackland.com.br/") if original_url else None

                insert_data.append((
                    item.get("LineName"),
                    route_name,
                    item.get("Direction"),
                    item.get("RealVehicle"),
                    url,
                    hoje
                ))

            try:
                for record in insert_data:
                    cursor.execute("""
                        INSERT IGNORE INTO informacoes (
                            LineName, RouteName, Direction, RealVehicle, url, data_execucao
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                    """, record)
                conn.commit()
                print(f"✅ {cursor.rowcount} violações salvas para {hoje}.")
            except mysql.connector.Error as db_err:
                if db_err.errno == 1062:
                    print("⚠️ Algumas rotas já estavam salvas (duplicatas ignoradas).")
                else:
                    print("❌ Erro no banco de dados:", db_err)

        conn.close()

    except requests.exceptions.RequestException as e:
        print("❌ Erro na requisição:", e)

def refresh_mv():
    try:
        conn = mysql.connector.connect(
            host=os.getenv("POWERBI_DB_HOST"),
            database=os.getenv("POWERBI_DB_NAME"),
            user=os.getenv("POWERBI_DB_USER"),
            password=os.getenv("POWERBI_DB_PASSWORD")
        )
        cursor = conn.cursor()

        print(f"🔄 Atualizando a Materialized View (MV) às {datetime.now()}...")

        cursor.execute("TRUNCATE TABLE informacoes_com_cliente_mv;")
        conn.commit()

        # Processar em lotes menores para evitar timeouts no servidor
        cursor.execute("SELECT COUNT(*) FROM informacoes;")
        total = cursor.fetchone()[0]
        batch_size = 500
        offset = 0

        while offset < total:
            cursor.execute("SELECT id FROM informacoes ORDER BY id LIMIT %s OFFSET %s", (batch_size, offset))
            ids = [row[0] for row in cursor.fetchall()]
            if not ids:
                break

            placeholders = ",".join(["%s"] * len(ids))

            insert_sql = f"""
                INSERT INTO informacoes_com_cliente_mv
                SELECT 
                    i.id,
                    i.LineName,
                    i.RouteName,
                    i.Direction,
                    i.RealVehicle,
                    i.data_execucao,
                    i.url,
                    i.violation_type,
                    COALESCE(h.client_name, g.client_name) AS client_name,
                    h.real_departure,
                    h.real_arrival,
                    h.id AS id_grade
                FROM 
                    u834686159_powerbi.informacoes i
                JOIN 
                    u834686159_powerbi.graderumocerto g 
                    ON TRIM(LOWER(i.RouteName)) = TRIM(LOWER(g.route_name))
                JOIN (
                    -- pega apenas o registro mais recente por rota/data na historico_grades
                    SELECT hg.*
                    FROM u834686159_powerbi.historico_grades hg
                    JOIN (
                        SELECT TRIM(LOWER(route_name)) AS route_name_norm, data_registro, MAX(id) AS max_id
                        FROM u834686159_powerbi.historico_grades
                        GROUP BY TRIM(LOWER(route_name)), data_registro
                    ) latest
                    ON TRIM(LOWER(hg.route_name)) = latest.route_name_norm AND hg.data_registro = latest.data_registro AND hg.id = latest.max_id
                ) h ON TRIM(LOWER(i.RouteName)) = TRIM(LOWER(h.route_name)) AND i.data_execucao = h.data_registro
                WHERE i.id IN ({placeholders}) AND h.id IS NOT NULL AND h.real_departure IS NOT NULL;
            """

            cursor.execute(insert_sql, tuple(ids))
            conn.commit()

            offset += batch_size

        print("✅ MV atualizada com sucesso.")
        conn.close()

    except Exception as e:
        print(f"❌ Erro ao atualizar a MV: {e}")

def verificar_violações_por_velocidade(token):
    def conectar_mysql():
        return mysql.connector.connect(
            host=os.getenv("POWERBI_DB_HOST"),
            database=os.getenv("POWERBI_DB_NAME"),
            user=os.getenv("POWERBI_DB_USER"),
            password=os.getenv("POWERBI_DB_PASSWORD")
        )

    parana_tz = pytz.timezone("America/Sao_Paulo")

    conn = conectar_mysql()
    cursor = conn.cursor(dictionary=True)

    batch_size = 10  # Reduzido para evitar timeout
    offset = 0
    lote = 1
    while True:
        print(f"🔹 Processando lote {lote} (offset {offset})...")
        cursor.execute("""
            SELECT id AS informacoes_id, RealVehicle, real_departure, real_arrival, RouteName, violation_type, id_grade
            FROM informacoes_com_cliente_mv
            WHERE real_departure IS NOT NULL AND real_arrival IS NOT NULL
            LIMIT %s OFFSET %s
        """, (batch_size, offset))
        registros = cursor.fetchall()
        if not registros:
            break

        for reg in registros:
            if reg.get('violation_type'):
                print(f"⏩ Pulando {reg['RouteName']} ({reg['RealVehicle']}) — violação já registrada: {reg['violation_type']}")
                continue

            try:
                informacoes_id = reg.get('informacoes_id')
                id_grade = reg.get('id_grade')

                if id_grade is None:
                    print(f"⚠️ ID_GRADE ausente na MV para informacoes_id={informacoes_id}; marcando como inconsistente.")
                    cursor.execute("UPDATE informacoes SET violation_type = %s WHERE id = %s", ("Dados Inconsistentes (grade ausente)", informacoes_id))
                    conn.commit()
                    continue

                validate_cursor = conn.cursor()
                validate_cursor.execute("SELECT COUNT(*) FROM u834686159_powerbi.historico_grades WHERE id = %s", (id_grade,))
                exists = validate_cursor.fetchone()[0] > 0
                validate_cursor.close()

                if not exists:
                    print(f"⚠️ Grade id={id_grade} não encontrada para informacoes_id={informacoes_id}; marcando como inconsistente.")
                    cursor.execute("UPDATE informacoes SET violation_type = %s WHERE id = %s", ("Dados Inconsistentes (grade ausente)", informacoes_id))
                    conn.commit()
                    continue

                vehicle_code = reg['RealVehicle']
                start = reg['real_departure']
                end = reg['real_arrival']
                route_name = reg['RouteName']

                if not (vehicle_code and start and end):
                    continue

                start_dt = parser.parse(start, dayfirst=True) if isinstance(start, str) else start
                end_dt = parser.parse(end, dayfirst=True) if isinstance(end, str) else end

                if getattr(start_dt, 'tzinfo', None) is None:
                    start_dt = parana_tz.localize(start_dt)
                else:
                    start_dt = start_dt.astimezone(parana_tz)
                if getattr(end_dt, 'tzinfo', None) is None:
                    end_dt = parana_tz.localize(end_dt)
                else:
                    end_dt = end_dt.astimezone(parana_tz)

                start_utc = start_dt.astimezone(pytz.utc)
                end_utc = end_dt.astimezone(pytz.utc)

                payload = {
                    "TrackedUnitType": 1,
                    "TrackedUnitIntegrationCode": vehicle_code,
                    "StartDatePosition": start_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "EndDatePosition": end_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                }

                headers = {"Authorization": f"Bearer {token}"}

                response = requests.post(
                    "https://integration.systemsatx.com.br/Controlws/HistoryPosition/List",
                    json=payload,
                    headers=headers
                )

                if response.status_code == 204:
                    continue
                elif response.status_code == 200 and response.content:
                    try:
                        positions = response.json()
                    except Exception:
                        continue
                else:
                    continue

                violacao = "Desvio de Rota"
                for pos in positions:
                    if pos.get("Velocity", 0) > 70:
                        violacao = "Velocidade Excedida"
                        break

                try:
                    conn.ping(reconnect=True)
                except Exception:
                    conn = conectar_mysql()
                    cursor = conn.cursor(dictionary=True)

                cursor.execute("UPDATE informacoes SET violation_type = %s WHERE id = %s", (violacao, informacoes_id))
                conn.commit()

                time.sleep(1)

            except Exception as e:
                print(f"💥 Erro inesperado na rota {reg.get('RouteName')} ({reg.get('RealVehicle')}): {e}")
                continue

        offset += batch_size
        lote += 1

    conn.close()

def iniciar_agendador():
    scheduler = BackgroundScheduler()
    scheduler.add_job(refresh_mv, 'interval', hours=1, id='refresh_mv_job', next_run_time=datetime.now())
    scheduler.start()
    print("⏱️ Agendador de atualização da MV iniciado.")

    atexit.register(lambda: scheduler.shutdown(wait=True))

if __name__ == '__main__':
    token = obter_token()
    if token:
        refresh_mv()  # Atualiza a MV primeiro
        routeviolation(token)
        verificar_violações_por_velocidade(token)
        iniciar_agendador()
        try:
            while True:
                time.sleep(60)
        except (KeyboardInterrupt, SystemExit):
            print("🛑 Encerrando o script...")
    else:
        print("❌ Não foi possível obter o token.")