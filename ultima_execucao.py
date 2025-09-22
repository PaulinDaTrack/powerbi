__all__ = ['atualizar_ultima_execucao']

import os
from dotenv import load_dotenv
load_dotenv()

import mysql.connector
from mysql.connector import pooling
from datetime import datetime
import pytz

print("POWERBI_DB_HOST:", os.getenv("POWERBI_DB_HOST"))
print("POWERBI_DB_USER:", os.getenv("POWERBI_DB_USER"))
print("POWERBI_DB_PASSWORD:", os.getenv("POWERBI_DB_PASSWORD"))
print("POWERBI_DB_NAME:", os.getenv("POWERBI_DB_NAME"))

db_config = {
    "host": os.getenv("POWERBI_DB_HOST"),
    "database": os.getenv("POWERBI_DB_NAME"),
    "user": os.getenv("POWERBI_DB_USER"),
    "password": os.getenv("POWERBI_DB_PASSWORD"),
}
connection_pool = pooling.MySQLConnectionPool(pool_name="mypool", pool_size=5, **db_config)

def atualizar_ultima_execucao():
    try:
        conn = connection_pool.get_connection()
        cursor = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS ultima_execucao (
            id INT PRIMARY KEY,
            last_execution DATETIME
        );
        """
        cursor.execute(create_table_query)
        
        upsert_query = """
        INSERT INTO ultima_execucao (id, last_execution)
        VALUES (1, %s)
        ON DUPLICATE KEY UPDATE last_execution = VALUES(last_execution)
        """
        parana_tz = pytz.timezone("America/Sao_Paulo")
        current_time = datetime.now(parana_tz)
        cursor.execute(upsert_query, (current_time,))
        
        conn.commit()
    except mysql.connector.Error as err:
        print("Erro ao conectar no banco de dados:", err)
    finally:
        cursor.close()
        conn.close()
    print("Última execução atualizada para:", current_time)

if __name__ == '__main__':
    atualizar_ultima_execucao()