def preencher_tabela_aluno(data_execucao):
    import pandas as pd
    data_str = data_execucao.strftime('%Y-%m-%d')

    # Usa o pool de conexão existente (mysql.connector) em vez de SQLAlchemy
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM Veiculo WHERE Data_Execucao = %s", (data_str,))
    veiculo_rows = cursor.fetchall()
    cursor.execute("SELECT * FROM Escola WHERE Data_Execucao = %s", (data_str,))
    escola_rows = cursor.fetchall()
    cursor.close()

    veiculo_df = pd.DataFrame(veiculo_rows) if veiculo_rows else pd.DataFrame()
    escola_df = pd.DataFrame(escola_rows) if escola_rows else pd.DataFrame()

    if veiculo_df.empty and escola_df.empty:
        conn.close()
        return

    alunos = sorted(set(veiculo_df['Matricula'].unique() if not veiculo_df.empty else []).union(
                    set(escola_df['Matricula'].unique() if not escola_df.empty else [])))

    for matricula in alunos:
        veic_logs = veiculo_df[veiculo_df['Matricula'] == matricula].sort_values('EventDate') if not veiculo_df.empty else pd.DataFrame()
        escola_logs = escola_df[escola_df['Matricula'] == matricula].sort_values('EventDate') if not escola_df.empty else pd.DataFrame()
        # Processar mesmo se não houver logs de veículo, desde que haja logs da escola
        if veic_logs.empty and escola_logs.empty:
            continue
        escola_nome = (escola_logs['Nome'].mode()[0] if not escola_logs.empty else "COL.ESTAD.DJALMA MARINHO")
        veiculo_placa = veic_logs['Placa'].mode()[0] if not veic_logs.empty else None

        # Somente preparar datetime/agrupamentos se houver logs de veículo
        agrupamentos = []
        if not veic_logs.empty:
            veic_logs['EventDate_dt'] = pd.to_datetime(veic_logs['EventDate'])
            veic_logs = veic_logs.sort_values('EventDate_dt').reset_index(drop=True)
            agrupamentos = _split_by_gap(veic_logs, 'EventDate_dt', GAP_SECONDS)

        entrada_ida_veic = saida_ida_veic = entrada_volta_veic = saida_volta_veic = None
        if len(agrupamentos) >= 1:
            ida_agrup = agrupamentos[0]
            entrada_ida_veic = None
            saida_ida_veic = None
            if len(ida_agrup) == 1:
                entrada_ida_veic = ida_agrup['EventDate'].iloc[0]
            else:
                entrada = ida_agrup['EventDate'].min()
                saida = ida_agrup['EventDate'].max()
                if entrada != saida:
                    entrada_ida_veic = entrada
                    saida_ida_veic = saida
                else:
                    entrada_ida_veic = entrada
        if len(agrupamentos) >= 2:
            volta_agrup = agrupamentos[1]
            entrada_volta_veic = None
            saida_volta_veic = None
            if len(volta_agrup) == 1:
                entrada_volta_veic = volta_agrup['EventDate'].iloc[0]
            else:
                entrada = volta_agrup['EventDate'].min()
                saida = volta_agrup['EventDate'].max()
                if entrada != saida:
                    entrada_volta_veic = entrada
                    saida_volta_veic = saida
                else:
                    entrada_volta_veic = entrada
        entrada_escola = escola_logs['EventDate'].min() if not escola_logs.empty else None
        saida_escola = escola_logs['EventDate'].max() if not escola_logs.empty else None
        if len(escola_logs) == 1:
            saida_escola = None
        elif entrada_escola == saida_escola:
            entrada_escola = None
            saida_escola = None
        if entrada_escola and not saida_escola and entrada_volta_veic:
            try:
                from datetime import datetime, timedelta
                saida_escola = (pd.to_datetime(entrada_volta_veic) - timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                pass

        (
            entrada_ida_veic,
            saida_ida_veic,
            entrada_escola,
            saida_escola,
            entrada_volta_veic,
            saida_volta_veic
        ) = ajustar_horarios_pelo_padrao(
            matricula=matricula,
            data_execucao=data_execucao,
            entrada_ida=entrada_ida_veic,
            saida_ida=saida_ida_veic,
            entrada_escola=entrada_escola,
            saida_escola=saida_escola,
            entrada_volta=entrada_volta_veic,
            saida_volta=saida_volta_veic
        )

        (
            entrada_ida_veic,
            saida_ida_veic,
            entrada_escola,
            saida_escola,
            entrada_volta_veic,
            saida_volta_veic
        ) = inferir_horarios_por_semelhanca(
            data_execucao=data_execucao,
            placa=veiculo_placa,
            entrada_ida=entrada_ida_veic,
            saida_ida=saida_ida_veic,
            entrada_escola=entrada_escola,
            saida_escola=saida_escola,
            entrada_volta=entrada_volta_veic,
            saida_volta=saida_volta_veic
        )

        (
            entrada_ida_veic,
            saida_ida_veic,
            entrada_escola,
            saida_escola,
            entrada_volta_veic,
            saida_volta_veic
        ) = garantir_ordem_cronologica_global(
            entrada_ida_veic,
            saida_ida_veic,
            entrada_escola,
            saida_escola,
            entrada_volta_veic,
            saida_volta_veic
        )

        (
            entrada_ida_veic,
            saida_ida_veic,
            entrada_escola,
            saida_escola,
            entrada_volta_veic,
            saida_volta_veic
        ) = ancorar_no_presente(
            data_execucao,
            entrada_ida_veic,
            saida_ida_veic,
            entrada_escola,
            saida_escola,
            entrada_volta_veic,
            saida_volta_veic
        )

        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO Aluno (
                Matricula, Escola, Veiculo,
                Entrada_Ida_Veiculo, Saida_Ida_Veiculo,
                Entrada_Escola, Saida_Escola,
                Entrada_Volta_Veiculo, Saida_Volta_Veiculo,
                Data_Execucao
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                Entrada_Ida_Veiculo = VALUES(Entrada_Ida_Veiculo),
                Saida_Ida_Veiculo = VALUES(Saida_Ida_Veiculo),
                Entrada_Escola = VALUES(Entrada_Escola),
                Saida_Escola = VALUES(Saida_Escola),
                Entrada_Volta_Veiculo = VALUES(Entrada_Volta_Veiculo),
                Saida_Volta_Veiculo = VALUES(Saida_Volta_Veiculo)
        """, (
            matricula,
            escola_nome,
            veiculo_placa,
            entrada_ida_veic,
            saida_ida_veic,
            entrada_escola,
            saida_escola,
            entrada_volta_veic,
            saida_volta_veic,
            data_execucao.strftime('%Y-%m-%d')
        ))
        cursor.close()
    conn.commit()
    conn.close()
def criar_tabela_aluno():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Aluno (
            id INT AUTO_INCREMENT PRIMARY KEY,
            Matricula VARCHAR(50) NOT NULL,
            Escola VARCHAR(255) NOT NULL,
            Veiculo VARCHAR(20) DEFAULT NULL,
            Entrada_Ida_Veiculo DATETIME,
            Saida_Ida_Veiculo DATETIME,
            Entrada_Escola DATETIME,
            Saida_Escola DATETIME,
            Entrada_Volta_Veiculo DATETIME,
            Saida_Volta_Veiculo DATETIME,
            Data_Execucao DATE NOT NULL,
            UNIQUE KEY uniq_aluno_dia (Matricula, Data_Execucao)
        )
    """)
    try:
        cursor.execute("CREATE UNIQUE INDEX uniq_aluno_dia ON Aluno (Matricula, Data_Execucao)")
    except Exception:
        pass
    conn.commit()
    cursor.close()
    conn.close()

import mysql.connector
from mysql.connector import pooling
import os
from dotenv import load_dotenv

import requests

from typing import List, Optional

API_URL = "https://integration.systemsatx.com.br/Controlws/HistoryPosition/List"
GAP_SECONDS = 600  # 10 minutos

def _ajustar_timestamp_iso_para_local(dt_str: Optional[str], shift_hours: int = 3) -> Optional[str]:
    """
    Converte string ISO para '%Y-%m-%d %H:%M:%S' ajustando fuso (ex.: UTC-3).
    Em caso de falha, retorna o valor original.
    """
    if not dt_str:
        return dt_str
    try:
        import dateutil.parser
        from datetime import timedelta
        dt = dateutil.parser.isoparse(dt_str) - timedelta(hours=shift_hours)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return dt_str

def _derivar_data_execucao_do_evento(event_date_str: Optional[str], fallback_date) -> str:
    """
    Deriva 'YYYY-MM-DD' a partir do EventDate (ajustado UTC-3). Se falhar, usa a data do parâmetro fallback_date.
    """
    try:
        import dateutil.parser
        from datetime import timedelta
        dt = dateutil.parser.isoparse(event_date_str) - timedelta(hours=3)
        return dt.strftime('%Y-%m-%d')
    except Exception:
        return fallback_date.strftime('%Y-%m-%d')

def _split_by_gap(df, time_col: str, gap_seconds: int = GAP_SECONDS) -> List:
    """
    Divide um DataFrame em subgrupos quando o gap entre linhas adjacentes excede gap_seconds.
    Retorna lista de DataFrames (subgrupos).
    """
    import pandas as pd
    if df is None or len(df) == 0:
        return []
    temp = df.sort_values(time_col).reset_index(drop=True)
    temp['__gap__'] = temp[time_col].diff().dt.total_seconds().fillna(0)
    starts = [0] + temp.index[(temp['__gap__'] > gap_seconds)].tolist()
    ends = starts[1:] + [len(temp)]
    return [temp.iloc[s:e].drop(columns='__gap__') for s, e in zip(starts, ends) if not temp.iloc[s:e].empty]

load_dotenv()

DB_HOST = os.getenv('POWERBI_DB_HOST')
DB_USER = os.getenv('POWERBI_DB_USER')
DB_PASSWORD = os.getenv('POWERBI_DB_PASSWORD')
DB_NAME = os.getenv('POWERBI_DB_NAME')

# HORÁRIOS PADRAO POR MATRÍCULA
HORARIOS_PADRAO = {
    "5809670":   {"ida_entrada":"06:32","ida_saida":"06:33","escola_entrada":"12:10","escola_saida":"12:14","volta_entrada":"12:15","volta_saida":"12:17"},
    "19808897":  {"ida_entrada":"06:39","ida_saida":"06:46","escola_entrada":"06:47","escola_saida":"12:17","volta_entrada":"12:18","volta_saida":"12:27"},
    "11563534":  {"ida_entrada":"12:42","ida_saida":"12:47","escola_entrada":"12:48","escola_saida":"18:14","volta_entrada":"18:15","volta_saida":"18:24"},
    "14599025":  {"ida_entrada":"12:40","ida_saida":"12:48","escola_entrada":"12:49","escola_saida":"18:17","volta_entrada":"18:17","volta_saida":"18:20"},
    "14611467":  {"ida_entrada":"12:44","ida_saida":"12:45","escola_entrada":"12:49","escola_saida":"18:15","volta_entrada":"18:16","volta_saida":"18:17"},
    "14612269":  {"ida_entrada":"12:40","ida_saida":"12:48","escola_entrada":"12:49","escola_saida":"18:14","volta_entrada":"18:15","volta_saida":"18:16"},
    "16498552":  {"ida_entrada":"12:42","ida_saida":"12:47","escola_entrada":"12:48","escola_saida":"18:14","volta_entrada":"18:15","volta_saida":"18:20"},
    "183317":    {"ida_entrada":"12:40","ida_saida":"12:47","escola_entrada":"12:49","escola_saida":"18:16","volta_entrada":"18:17","volta_saida":"18:25"},
    "4233022":   {"ida_entrada":"06:35","ida_saida":"06:37","escola_entrada":"06:42","escola_saida":"12:14","volta_entrada":"12:15","volta_saida":"12:23"},
    "913443":    {"ida_entrada":"06:35","ida_saida":"06:36","escola_entrada":"06:55","escola_saida":"12:13","volta_entrada":"12:15","volta_saida":"12:22"},
    "17519090":  {"ida_entrada":"06:40","ida_saida":"06:47","escola_entrada":"06:57","escola_saida":"12:20","volta_entrada":"12:21","volta_saida":"12:31"},
    "30893251":  {"ida_entrada":"06:40","ida_saida":"06:46","escola_entrada":"06:47","escola_saida":"12:21","volta_entrada":"12:22","volta_saida":"12:31"},
    "7906753":   {"ida_entrada":"06:38","ida_saida":"06:47","escola_entrada":"06:47","escola_saida":"12:17","volta_entrada":"12:22","volta_saida":"12:24"},
    "933196":    {"ida_entrada":"06:39","ida_saida":"06:47","escola_entrada":"06:47","escola_saida":"12:26","volta_entrada":"12:27","volta_saida":"12:28"},
    "14597596":  {"ida_entrada":"12:39","ida_saida":"12:46","escola_entrada":"12:46","escola_saida":"18:16","volta_entrada":"18:17","volta_saida":"18:18"},
    "5809017":   {"ida_entrada":"06:31","ida_saida":"06:32","escola_entrada":"06:59","escola_saida":"12:15","volta_entrada":"12:16","volta_saida":"12:25"},
    "863580":    {"ida_entrada":"06:28","ida_saida":"06:29","escola_entrada":"06:51","escola_saida":"12:16","volta_entrada":"12:17","volta_saida":"12:24"},
    "863660":    {"ida_entrada":"06:39","ida_saida":"06:48","escola_entrada":"06:49","escola_saida":"12:17","volta_entrada":"12:17","volta_saida":"12:19"},
    "2367271":   {"ida_entrada":"06:38","ida_saida":"06:45","escola_entrada":"06:50","escola_saida":"12:17","volta_entrada":"12:18","volta_saida":"12:27"},
    "12265855":  {"ida_entrada":"06:28","ida_saida":"06:35","escola_entrada":"06:56","escola_saida":"12:14","volta_entrada":"12:15","volta_saida":"12:24"},
    "12562698":  {"ida_entrada":"12:44","ida_saida":"12:45","escola_entrada":"12:45","escola_saida":"18:18","volta_entrada":"18:19","volta_saida":"18:20"},
    "17205346":  {"ida_entrada":"06:39","ida_saida":"06:40","escola_entrada":"06:43","escola_saida":"12:15","volta_entrada":"12:16","volta_saida":"12:17"},
    "6604079":   {"ida_entrada":"06:34","ida_saida":"06:39","escola_entrada":"06:44","escola_saida":"12:14","volta_entrada":"12:15","volta_saida":"12:21"},
    "19309253":  {"ida_entrada":"06:32","ida_saida":"06:39","escola_entrada":"06:56","escola_saida":"12:15","volta_entrada":"12:16","volta_saida":"12:26"},
    "12428428":  {"ida_entrada":"12:41","ida_saida":"12:49","escola_entrada":"12:49","escola_saida":"18:15","volta_entrada":"18:16","volta_saida":"18:23"},
    "11852927":  {"ida_entrada":"06:45","ida_saida":"06:48","escola_entrada":"06:51","escola_saida":"12:15","volta_entrada":"12:16","volta_saida":"12:26"},
    "12988318":  {"ida_entrada":"06:41","ida_saida":"06:48","escola_entrada":"06:48","escola_saida":"12:27","volta_entrada":"12:28","volta_saida":"12:31"},
    "17109550":  {"ida_entrada":"06:41","ida_saida":"06:43","escola_entrada":"06:49","escola_saida":"12:27","volta_entrada":"12:28","volta_saida":"12:31"},
    "32191025":  {"ida_entrada":"06:41","ida_saida":"06:42","escola_entrada":"06:47","escola_saida":"12:17","volta_entrada":"12:21","volta_saida":"12:25"},
    "4247767":   {"ida_entrada":"06:40","ida_saida":"06:42","escola_entrada":"06:47","escola_saida":"12:26","volta_entrada":"12:27","volta_saida":"12:29"},
    "6855837":   {"ida_entrada":"06:39","ida_saida":"06:45","escola_entrada":"06:47","escola_saida":"12:16","volta_entrada":"12:16","volta_saida":"12:20"},
    "6856264":   {"ida_entrada":"06:38","ida_saida":"06:39","escola_entrada":"06:47","escola_saida":"12:15","volta_entrada":"12:16","volta_saida":"12:23"},
    "746712":    {"ida_entrada":"06:45","ida_saida":"06:47","escola_entrada":"06:48","escola_saida":"12:17","volta_entrada":"12:18","volta_saida":"12:20"},
    "7895239":   {"ida_entrada":"06:37","ida_saida":"06:46","escola_entrada":"06:49","escola_saida":"12:21","volta_entrada":"12:22","volta_saida":"12:23"},
    "7896022":   {"ida_entrada":"06:48","ida_saida":"06:53","escola_entrada":"06:55","escola_saida":"12:18","volta_entrada":"12:18","volta_saida":"12:23"},
    "810213":    {"ida_entrada":"06:39","ida_saida":"06:48","escola_entrada":"06:49","escola_saida":"12:18","volta_entrada":"12:18","volta_saida":"12:22"},
    "1366996":   {"ida_entrada":"06:41","ida_saida":"06:42","escola_entrada":"06:52","escola_saida":"12:26","volta_entrada":"12:27","volta_saida":"12:29"},
    "580994":    {"ida_entrada":"06:31","ida_saida":"06:39","escola_entrada":"06:52","escola_saida":"12:16","volta_entrada":"12:16","volta_saida":"12:23"},
    "5997778":   {"ida_entrada":"06:29","ida_saida":"06:35","escola_entrada":"06:56","escola_saida":"12:16","volta_entrada":"12:16","volta_saida":"12:31"},
    "9519327":   {"ida_entrada":"06:38","ida_saida":"06:42","escola_entrada":"06:44","escola_saida":"12:16","volta_entrada":"12:17","volta_saida":"12:23"},
    "11541611":  {"ida_entrada":"12:18","ida_saida":"12:26","escola_entrada":"12:27","escola_saida":"18:28","volta_entrada":"18:29","volta_saida":"18:34"},
    "1257957":   {"ida_entrada":"12:31","ida_saida":"12:33","escola_entrada":"12:40","escola_saida":"18:18","volta_entrada":"18:19","volta_saida":"18:24"},
    "14297363":  {"ida_entrada":"12:37","ida_saida":"12:39","escola_entrada":"12:44","escola_saida":"18:15","volta_entrada":"18:16","volta_saida":"18:18"},
    "14597456":  {"ida_entrada":"12:29","ida_saida":"12:30","escola_entrada":"12:40","escola_saida":"18:13","volta_entrada":"18:14","volta_saida":"18:16"},
    "16399333":  {"ida_entrada":"12:25","ida_saida":"12:34","escola_entrada":"12:44","escola_saida":"18:14","volta_entrada":"18:15","volta_saida":"18:24"},
    "16554886":  {"ida_entrada":"12:27","ida_saida":"12:35","escola_entrada":"12:37","escola_saida":"18:18","volta_entrada":"18:18","volta_saida":"18:20"},
    "171251":    {"ida_entrada":"12:34","ida_saida":"12:37","escola_entrada":"12:38","escola_saida":"18:15","volta_entrada":"18:15","volta_saida":"18:20"},
    "17166359":  {"ida_entrada":"12:31","ida_saida":"12:39","escola_entrada":"12:40","escola_saida":"18:15","volta_entrada":"18:16","volta_saida":"18:32"},
    "17839388":  {"ida_entrada":"12:29","ida_saida":"12:35","escola_entrada":"12:40","escola_saida":"18:19","volta_entrada":"18:20","volta_saida":"18:25"},
    "19805596":  {"ida_entrada":"12:27","ida_saida":"12:36","escola_entrada":"12:38","escola_saida":"18:18","volta_entrada":"18:19","volta_saida":"18:28"},
    "201531":    {"ida_entrada":"06:45","ida_saida":"06:46","escola_entrada":"06:46","escola_saida":"12:17","volta_entrada":"12:18","volta_saida":"12:19"},
    "29915454":  {"ida_entrada":"12:31","ida_saida":"12:39","escola_entrada":"12:39","escola_saida":"18:18","volta_entrada":"18:19","volta_saida":"18:28"},
    "32184720":  {"ida_entrada":"12:27","ida_saida":"12:35","escola_entrada":"12:46","escola_saida":"18:19","volta_entrada":"18:20","volta_saida":"18:28"},
    "32185564":  {"ida_entrada":"12:27","ida_saida":"12:35","escola_entrada":"12:49","escola_saida":"18:19","volta_entrada":"18:20","volta_saida":"18:28"},
    "5946316":   {"ida_entrada":"06:29","ida_saida":"06:35","escola_entrada":"06:56","escola_saida":"12:16","volta_entrada":"12:17","volta_saida":"12:21"},
    "681823":    {"ida_entrada":"06:35","ida_saida":"06:37","escola_entrada":"06:37","escola_saida":"12:16","volta_entrada":"12:17","volta_saida":"12:21"},
    "7893368":   {"ida_entrada":"12:20","ida_saida":"12:21","escola_entrada":"12:22","escola_saida":"18:15","volta_entrada":"18:15","volta_saida":"18:19"},
    "7897851":   {"ida_entrada":"06:46","ida_saida":"06:54","escola_entrada":"06:56","escola_saida":"12:30","volta_entrada":"12:31","volta_saida":"12:37"},
    "9963959":   {"ida_entrada":"06:46","ida_saida":"06:47","escola_entrada":"06:48","escola_saida":"12:15","volta_entrada":"12:16","volta_saida":"12:18"},
    "9991766":   {"ida_entrada":"06:35","ida_saida":"06:40","escola_entrada":"06:46","escola_saida":"12:27","volta_entrada":"12:28","volta_saida":"12:33"},
    "10654919":  {"ida_entrada":"12:42","ida_saida":"12:44","escola_entrada":"12:45","escola_saida":"18:16","volta_entrada":"18:16","volta_saida":"18:18"},
    "14611661":  {"ida_entrada":"12:41","ida_saida":"12:45","escola_entrada":"12:46","escola_saida":"18:23","volta_entrada":"18:24","volta_saida":"18:28"},
    "1534315":   {"ida_entrada":"12:42","ida_saida":"12:45","escola_entrada":"12:51","escola_saida":"18:17","volta_entrada":"18:26","volta_saida":"18:29"},
    "1724510":   {"ida_entrada":"06:32","ida_saida":"06:39","escola_entrada":"06:42","escola_saida":"12:30","volta_entrada":"12:30","volta_saida":"12:37"},
    "5148720":   {"ida_entrada":"06:50","ida_saida":"06:56","escola_entrada":"06:57","escola_saida":"12:15","volta_entrada":"12:18","volta_saida":"12:21"},
    "5808690":   {"ida_entrada":"06:34","ida_saida":"06:39","escola_entrada":"06:46","escola_saida":"12:16","volta_entrada":"12:17","volta_saida":"12:20"},
    "686094":    {"ida_entrada":"06:26","ida_saida":"06:27","escola_entrada":"06:47","escola_saida":"12:15","volta_entrada":"12:16","volta_saida":"12:18"},
    "7977561":   {"ida_entrada":"12:46","ida_saida":"12:47","escola_entrada":"18:18","escola_saida":"18:19","volta_entrada":"18:20","volta_saida":"18:22"},
    "873682":    {"ida_entrada":"06:50","ida_saida":"06:53","escola_entrada":"06:54","escola_saida":"12:16","volta_entrada":"12:17","volta_saida":"12:18"},
    "9966095":   {"ida_entrada":"12:43","ida_saida":"12:48","escola_entrada":"12:49","escola_saida":"18:14","volta_entrada":"18:15","volta_saida":"18:20"},
    "10257286":  {"ida_entrada":"12:31","ida_saida":"12:39","escola_entrada":"12:40","escola_saida":"18:19","volta_entrada":"18:20","volta_saida":"18:24"},
    "1390970":   {"ida_entrada":"06:43","ida_saida":"06:46","escola_entrada":"06:47","escola_saida":"12:16","volta_entrada":"12:17","volta_saida":"12:19"},
    "4395332":   {"ida_entrada":"12:42","ida_saida":"12:47","escola_entrada":"12:48","escola_saida":"18:15","volta_entrada":"18:16","volta_saida":"18:20"},
    "11294231":  {"ida_entrada":"06:44","ida_saida":"06:48","escola_entrada":"06:50","escola_saida":"12:15","volta_entrada":"12:16","volta_saida":"12:19"},
    "1320806":   {"ida_entrada":"06:40","ida_saida":"06:45","escola_entrada":"06:49","escola_saida":"12:15","volta_entrada":"12:16","volta_saida":"12:19"},
    "16211024":  {"ida_entrada":"06:40","ida_saida":"06:43","escola_entrada":"06:44","escola_saida":"12:27","volta_entrada":"12:28","volta_saida":"12:31"},
    "27959039":  {"ida_entrada":"12:39","ida_saida":"12:46","escola_entrada":"12:48","escola_saida":"18:17","volta_entrada":"18:17","volta_saida":"18:18"},
    "33754260":  {"ida_entrada":"12:44","ida_saida":"12:45","escola_entrada":"12:46","escola_saida":"18:15","volta_entrada":"18:15","volta_saida":"18:17"},
    "4249190":   {"ida_entrada":"06:39","ida_saida":"06:47","escola_entrada":"06:52","escola_saida":"12:16","volta_entrada":"12:17","volta_saida":"12:23"},
    "4269752":   {"ida_entrada":"06:26","ida_saida":"06:35","escola_entrada":"06:46","escola_saida":"12:17","volta_entrada":"12:18","volta_saida":"12:27"},
    "9208220":   {"ida_entrada":"06:39","ida_saida":"06:43","escola_entrada":"06:44","escola_saida":"12:16","volta_entrada":"12:17","volta_saida":"12:19"},
    "935566":    {"ida_entrada":"12:45","ida_saida":"12:46","escola_entrada":"12:46","escola_saida":"18:14","volta_entrada":"18:15","volta_saida":"18:17"},
    "9441550":   {"ida_entrada":"06:39","ida_saida":"06:44","escola_entrada":"06:45","escola_saida":"12:17","volta_entrada":"12:18","volta_saida":"12:20"},
    "12428355":  {"ida_entrada":"12:43","ida_saida":"12:48","escola_entrada":"12:49","escola_saida":"18:16","volta_entrada":"18:17","volta_saida":"18:22"},
    "14598690":  {"ida_entrada":"12:42","ida_saida":"12:46","escola_entrada":"12:49","escola_saida":"18:17","volta_entrada":"18:18","volta_saida":"18:20"},
    "9962685":   {"ida_entrada":"12:39","ida_saida":"12:48","escola_entrada":"12:49","escola_saida":"18:16","volta_entrada":"18:17","volta_saida":"18:25"},
    "16678119":  {"ida_entrada":"06:41","ida_saida":"06:45","escola_entrada":"06:46","escola_saida":"12:26","volta_entrada":"12:27","volta_saida":"12:30"},
    "30893260":  {"ida_entrada":"12:31","ida_saida":"12:38","escola_entrada":"12:39","escola_saida":"18:18","volta_entrada":"18:18","volta_saida":"18:22"},
    "633272":    {"ida_entrada":"06:46","ida_saida":"06:47","escola_entrada":"06:48","escola_saida":"12:15","volta_entrada":"12:16","volta_saida":"12:18"},
    "7899242":   {"ida_entrada":"06:49","ida_saida":"06:52","escola_entrada":"06:53","escola_saida":"12:15","volta_entrada":"12:16","volta_saida":"12:18"},
    "7893554":   {"ida_entrada":"06:49","ida_saida":"06:55","escola_entrada":"06:56","escola_saida":"12:14","volta_entrada":"12:14","volta_saida":"12:20"},
    "8852118":   {"ida_entrada":"06:46","ida_saida":"06:47","escola_entrada":"06:48","escola_saida":"12:16","volta_entrada":"12:16","volta_saida":"12:18"},
    "12734049":  {"ida_entrada":"12:41","ida_saida":"12:45","escola_entrada":"12:46","escola_saida":"18:27","volta_entrada":"18:28","volta_saida":"18:32"},
    "15417354":  {"ida_entrada":"12:39","ida_saida":"12:41","escola_entrada":"12:42","escola_saida":"18:16","volta_entrada":"18:16","volta_saida":"18:18"},
    "27608910":  {"ida_entrada":"06:40","ida_saida":"06:43","escola_entrada":"06:44","escola_saida":"12:15","volta_entrada":"12:16","volta_saida":"12:19"},
    "5999010":   {"ida_entrada":"06:39","ida_saida":"06:42","escola_entrada":"06:43","escola_saida":"12:16","volta_entrada":"12:16","volta_saida":"12:19"},
    "686517":    {"ida_entrada":"12:27","ida_saida":"12:28","escola_entrada":"12:29","escola_saida":"18:15","volta_entrada":"18:16","volta_saida":"18:18"},
    "12508715":  {"ida_entrada":"06:46","ida_saida":"06:57","escola_entrada":"06:58","escola_saida":"12:16","volta_entrada":"12:16","volta_saida":"12:24"},
    "2373479":   {"ida_entrada":"12:28","ida_saida":"12:37","escola_entrada":"12:39","escola_saida":"18:17","volta_entrada":"18:17","volta_saida":"18:25"},
    "7894354":   {"ida_entrada":"06:39","ida_saida":"06:43","escola_entrada":"06:44","escola_saida":"12:16","volta_entrada":"12:16","volta_saida":"12:20"},
    "14708878":  {"ida_entrada":"12:42","ida_saida":"12:48","escola_entrada":"12:49","escola_saida":"18:16","volta_entrada":"18:17","volta_saida":"18:21"},
    "7902294":   {"ida_entrada":"12:35","ida_saida":"12:39","escola_entrada":"12:40","escola_saida":"18:15","volta_entrada":"18:15","volta_saida":"18:21"},
    "5809360":   {"ida_entrada":"06:39","ida_saida":"06:51","escola_entrada":"06:52","escola_saida":"12:16","volta_entrada":"12:16","volta_saida":"12:26"},
    "12428380":  {"ida_entrada":"12:40","ida_saida":"12:44","escola_entrada":"12:45","escola_saida":"18:19","volta_entrada":"18:19","volta_saida":"18:22"},
    "1258076":   {"ida_entrada":"12:40","ida_saida":"12:44","escola_entrada":"12:45","escola_saida":"18:29","volta_entrada":"18:30","volta_saida":"18:34"},
    "1263162":   {"ida_entrada":"12:38","ida_saida":"12:43","escola_entrada":"12:44","escola_saida":"18:28","volta_entrada":"18:29","volta_saida":"18:34"},
    "14293163":  {"ida_entrada":"12:38","ida_saida":"12:43","escola_entrada":"12:44","escola_saida":"18:27","volta_entrada":"18:28","volta_saida":"18:32"},
    "14304122":  {"ida_entrada":"12:36","ida_saida":"12:43","escola_entrada":"12:44","escola_saida":"18:18","volta_entrada":"18:29","volta_saida":"18:35"},
    "1588253":   {"ida_entrada":"12:36","ida_saida":"12:44","escola_entrada":"12:45","escola_saida":"18:17","volta_entrada":"18:18","volta_saida":"18:28"},
    "16495340":  {"ida_entrada":"12:43","ida_saida":"12:44","escola_entrada":"12:45","escola_saida":"18:14","volta_entrada":"18:32","volta_saida":"18:33"},
    "17145084":  {"ida_entrada":"12:40","ida_saida":"12:44","escola_entrada":"12:45","escola_saida":"18:16","volta_entrada":"18:16","volta_saida":"18:22"},
    "1765293":   {"ida_entrada":"12:41","ida_saida":"12:42","escola_entrada":"12:43","escola_saida":"18:14","volta_entrada":"18:15","volta_saida":"18:16"},
    "17808431":  {"ida_entrada":"12:40","ida_saida":"12:44","escola_entrada":"12:45","escola_saida":"18:15","volta_entrada":"18:34","volta_saida":"18:38"},
    "18808079":  {"ida_entrada":"12:40","ida_saida":"12:44","escola_entrada":"12:45","escola_saida":"18:32","volta_entrada":"18:32","volta_saida":"18:35"},
    "29853246":  {"ida_entrada":"12:41","ida_saida":"12:44","escola_entrada":"12:45","escola_saida":"18:16","volta_entrada":"18:33","volta_saida":"18:36"},
    "5875125":   {"ida_entrada":"12:41","ida_saida":"12:44","escola_entrada":"12:45","escola_saida":"18:17","volta_entrada":"18:28","volta_saida":"18:31"},
    "884342":    {"ida_entrada":"12:40","ida_saida":"12:43","escola_entrada":"12:44","escola_saida":"18:33","volta_entrada":"18:34","volta_saida":"18:36"},
    "13391595":  {"ida_entrada":"12:45","ida_saida":"12:50","escola_entrada":"12:51","escola_saida":"18:16","volta_entrada":"18:16","volta_saida":"18:22"},
    "1706262":   {"ida_entrada":"12:42","ida_saida":"12:46","escola_entrada":"12:47","escola_saida":"18:16","volta_entrada":"18:16","volta_saida":"18:22"},
    "12428517":  {"ida_entrada":"12:42","ida_saida":"12:48","escola_entrada":"12:49","escola_saida":"18:16","volta_entrada":"18:17","volta_saida":"18:20"},
    "9962120":   {"ida_entrada":"06:50","ida_saida":"06:52","escola_entrada":"06:53","escola_saida":"12:14","volta_entrada":"12:14","volta_saida":"12:16"},
}

THRESHOLD_MINUTOS_DERIVA = 90

from datetime import datetime, timedelta
from statistics import median

def _combinar_data_hora(data_dt, hora_str):
    if not hora_str:
        return None
    try:
        hh, mm = map(int, str(hora_str).split(':')[:2])
        return datetime(data_dt.year, data_dt.month, data_dt.day, hh, mm, 0)
    except Exception:
        return None

def _to_datetime_or_none(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    s = str(val)
    try:
        return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
    except Exception:
        pass
    try:
        s2 = s.replace('Z', '')
        if '.' in s2:
            s2 = s2.split('.')[0]
        return datetime.fromisoformat(s2)
    except Exception:
        return None

def _fmt(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S') if isinstance(dt, datetime) else None

def ajustar_horarios_pelo_padrao(
    matricula,
    data_execucao,
    entrada_ida,
    saida_ida,
    entrada_escola,
    saida_escola,
    entrada_volta,
    saida_volta
):
    padrao = HORARIOS_PADRAO.get(str(matricula))
    if not padrao:
        return (
            _fmt(_to_datetime_or_none(entrada_ida)),
            _fmt(_to_datetime_or_none(saida_ida)),
            _fmt(_to_datetime_or_none(entrada_escola)),
            _fmt(_to_datetime_or_none(saida_escola)),
            _fmt(_to_datetime_or_none(entrada_volta)),
            _fmt(_to_datetime_or_none(saida_volta)),
        )

    base = data_execucao if isinstance(data_execucao, datetime) else _to_datetime_or_none(data_execucao)

    ei = _to_datetime_or_none(entrada_ida)
    si = _to_datetime_or_none(saida_ida)
    ee = _to_datetime_or_none(entrada_escola)
    se = _to_datetime_or_none(saida_escola)
    ev = _to_datetime_or_none(entrada_volta)
    sv = _to_datetime_or_none(saida_volta)

    thr = timedelta(minutes=THRESHOLD_MINUTOS_DERIVA)

    def apply_pair(cur_in, cur_out, key_in, key_out):
        std_in = _combinar_data_hora(base, padrao.get(key_in))
        std_out = _combinar_data_hora(base, padrao.get(key_out))
        if cur_in is None and cur_out is None:
            return None, None
        if cur_in is None:
            cur_in = std_in
        if cur_out is None:
            cur_out = std_out
        if std_in and cur_in and abs(cur_in - std_in) > thr:
            cur_in = std_in
        if std_out and cur_out and abs(cur_out - std_out) > thr:
            cur_out = std_out
        if cur_in and cur_out and cur_in > cur_out:
            if std_out and std_out >= cur_in:
                cur_out = std_out
            else:
                cur_out = cur_in + timedelta(minutes=30)
        return cur_in, cur_out

    ei, si = apply_pair(ei, si, 'ida_entrada', 'ida_saida')
    ee, se = apply_pair(ee, se, 'escola_entrada', 'escola_saida') if not (ee is None and se is None) else (None, None)
    ev, sv = apply_pair(ev, sv, 'volta_entrada', 'volta_saida')

    return (_fmt(ei), _fmt(si), _fmt(ee), _fmt(se), _fmt(ev), _fmt(sv))


def _to_minutos(hhmm):
    if not hhmm: return None
    try:
        hh, mm = map(int, str(hhmm).split(':')[:2])
        return hh*60 + mm
    except Exception:
        return None

def _dt_from_minutos(base_date, minutos):
    if minutos is None or base_date is None:
        return None
    hh = minutos // 60
    mm = minutos % 60
    return datetime(base_date.year, base_date.month, base_date.day, hh, mm, 0)

def _calcular_medianas_horarios_padrao():
    campos = ['ida_entrada','ida_saida','escola_entrada','escola_saida','volta_entrada','volta_saida']
    buckets = {'manha': {c: [] for c in campos}, 'tarde': {c: [] for c in campos}}
    for v in HORARIOS_PADRAO.values():
        ent_escola_min = _to_minutos(v.get('escola_entrada'))
        ref_min = ent_escola_min
        if ref_min is None:
            for c in ['ida_entrada','volta_entrada','ida_saida','volta_saida','escola_saida']:
                ref_min = _to_minutos(v.get(c))
                if ref_min is not None:
                    break
        turno = 'manha' if (ref_min is not None and ref_min < 12*60) else 'tarde'
        for c in buckets[turno].keys():
            m = _to_minutos(v.get(c))
            if m is not None:
                buckets[turno][c].append(m)
    def _medianas(lista_por_campo):
        out = {}
        for k, lst in lista_por_campo.items():
            if lst:
                out[k] = int(median(lst))
        return out
    return {'manha': _medianas(buckets['manha']), 'tarde': _medianas(buckets['tarde'])}

SHIFT_MEDIANS = _calcular_medianas_horarios_padrao()

def inferir_horarios_por_semelhanca(
    data_execucao,
    placa,
    entrada_ida,
    saida_ida,
    entrada_escola,
    saida_escola,
    entrada_volta,
    saida_volta
):
    base = data_execucao if isinstance(data_execucao, datetime) else _to_datetime_or_none(data_execucao)

    ei = _to_datetime_or_none(entrada_ida)
    si = _to_datetime_or_none(saida_ida)
    ee = _to_datetime_or_none(entrada_escola)
    se = _to_datetime_or_none(saida_escola)
    ev = _to_datetime_or_none(entrada_volta)
    sv = _to_datetime_or_none(saida_volta)

    ref = ee or ei or ev or si or se or sv
    turno = 'manha' if (ref and ref.hour < 12) else 'tarde'
    meds = SHIFT_MEDIANS.get(turno, {})

    def fill_pair(cur_in, cur_out, key_in, key_out):
        if cur_in is None and cur_out is None:
            return None, None
        if cur_in is None:
            mi = meds.get(key_in)
            if mi is not None:
                cur_in = _dt_from_minutos(base, mi)
        if cur_out is None:
            mo = meds.get(key_out)
            if mo is not None:
                cur_out = _dt_from_minutos(base, mo)
        if cur_in and cur_out and cur_in > cur_out:
            cur_out = cur_in + timedelta(minutes=30)
        return cur_in, cur_out

    ei, si = fill_pair(ei, si, 'ida_entrada', 'ida_saida')
    ee, se = fill_pair(ee, se, 'escola_entrada', 'escola_saida')
    ev, sv = fill_pair(ev, sv, 'volta_entrada', 'volta_saida')

    return (_fmt(ei), _fmt(si), _fmt(ee), _fmt(se), _fmt(ev), _fmt(sv))

db_config = {
    "host": DB_HOST,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "database": DB_NAME,
}
connection_pool = pooling.MySQLConnectionPool(pool_name="mypool_tags", pool_size=5, **db_config)

def get_db_connection():
    return connection_pool.get_connection()

def criar_tabela_escola():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Escola (
            id INT AUTO_INCREMENT PRIMARY KEY,
            Nome VARCHAR(255) NOT NULL,
            EventDate DATETIME NOT NULL,
            UpdateDate DATETIME NOT NULL,
            Matricula VARCHAR(50) NOT NULL,
            Data_Execucao DATE NOT NULL,
            UNIQUE KEY uniq_escola_evento (EventDate, Matricula)
        )
    """)
    try:
        cursor.execute("CREATE UNIQUE INDEX uniq_escola_evento ON Escola (EventDate, Matricula)")
    except Exception:
        pass
    conn.commit()
    cursor.close()
    conn.close()

def criar_tabela_veiculo():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Veiculo (
            id INT AUTO_INCREMENT PRIMARY KEY,
            Placa VARCHAR(20) NOT NULL,
            EventDate DATETIME NOT NULL,
            UpdateDate DATETIME NOT NULL,
            Ignition BOOLEAN NOT NULL,
            Matricula VARCHAR(50) NOT NULL,
            Latitude DOUBLE NOT NULL,
            Longitude DOUBLE NOT NULL,
            Data_Execucao DATE NOT NULL,
            UNIQUE KEY uniq_veic_evento (Placa, Matricula, EventDate)
        )
    """)
    try:
        cursor.execute("CREATE UNIQUE INDEX uniq_veic_evento ON Veiculo (Placa, Matricula, EventDate)")
    except Exception:
        pass
    conn.commit()
    cursor.close()
    conn.close()


def consultar_api_escola(data_consulta, token=None):
    url = API_URL
    from datetime import datetime
    data_inicio = data_consulta.strftime('%Y-%m-%dT00:00:00.000Z')
    data_fim = data_consulta.strftime('%Y-%m-%dT23:59:59.595Z')
    payload = {
        "TrackedUnitType": 1,
        "TrackedUnitIntegrationCode": "COL.ESTAD.DJALMA MARINHO",
        "StartDatePosition": data_inicio,
        "EndDatePosition": data_fim
    }
    if token is None:
        from authtoken import obter_token
        token = obter_token()
    if not token:
        print("Não foi possível obter o token de autenticação.")
        return None
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        import dateutil.parser
        from datetime import timedelta
        dados = response.json()
        conn = get_db_connection()
        cursor = conn.cursor()
        if isinstance(dados, list):
            for item in dados:
                matricula = item.get('Driver')
                idevent = item.get('IdEvent')
                if (matricula is not None and str(matricula).strip() != "" and idevent == 65):
                    nome = item.get('TrackedUnit')
                    event_date_raw = item.get('EventDate')
                    update_date_raw = item.get('UpdateDate')

                    data_execucao_sql = _derivar_data_execucao_do_evento(event_date_raw, data_consulta)
                    if data_execucao_sql != data_consulta.strftime('%Y-%m-%d'):
                        continue
                    event_date = _ajustar_timestamp_iso_para_local(event_date_raw, 3)
                    update_date = _ajustar_timestamp_iso_para_local(update_date_raw, 3)

                    cursor.execute("""
                        INSERT INTO Escola (Nome, EventDate, UpdateDate, Matricula, Data_Execucao)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            Nome = VALUES(Nome),
                            UpdateDate = VALUES(UpdateDate)
                    """, (nome, event_date, update_date, matricula, data_execucao_sql))
        else:
            item = dados
            matricula = item.get('Driver')
            idevent = item.get('IdEvent')
            if (matricula is not None and str(matricula).strip() != "" and idevent == 65):
                nome = item.get('TrackedUnit')
                event_date_raw = item.get('EventDate')
                update_date_raw = item.get('UpdateDate')

                data_execucao_sql = _derivar_data_execucao_do_evento(event_date_raw, data_consulta)
                if data_execucao_sql != data_consulta.strftime('%Y-%m-%d'):
                    return dados
                event_date = _ajustar_timestamp_iso_para_local(event_date_raw, 3)
                update_date = _ajustar_timestamp_iso_para_local(update_date_raw, 3)

                cursor.execute("""
                    INSERT INTO Escola (Nome, EventDate, UpdateDate, Matricula, Data_Execucao)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        Nome = VALUES(Nome),
                        UpdateDate = VALUES(UpdateDate)
                """, (nome, event_date, update_date, matricula, data_execucao_sql))
        conn.commit()
        cursor.close()
        conn.close()
        return dados
    else:
        print("Erro na consulta:", response.status_code, response.text)
        return None

def consultar_api_veiculo(data_consulta, token=None):
    import pandas as pd
    url = API_URL
    placas = ["AXM9A53", "CUE2D20", "IUZ4F94"]
    if token is None:
        from authtoken import obter_token
        token = obter_token()
    if not token:
        print("Não foi possível obter o token de autenticação.")
        return None
    from datetime import datetime
    data_inicio = data_consulta.strftime('%Y-%m-%dT00:00:00.000Z')
    data_fim = data_consulta.strftime('%Y-%m-%dT23:59:59.595Z')
    todos_logs = []
    from datetime import datetime, timedelta, timezone
    import dateutil.parser
    for placa in placas:
        payload = {
            "TrackedUnitType": 1,
            "TrackedUnitIntegrationCode": placa,
            "StartDatePosition": data_inicio,
            "EndDatePosition": data_fim
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            dados = response.json()
            if isinstance(dados, list):
                for item in dados:
                    matricula = item.get('Driver')
                    idevent = item.get('IdEvent')
                    if (item.get('Ignition') == True and matricula is not None and str(matricula).strip() != "" and idevent == 65):
                        eventdate_raw = item.get('EventDate')
                        updatedate_raw = item.get('UpdateDate')
                        data_execucao_sql = _derivar_data_execucao_do_evento(eventdate_raw, data_consulta)
                        if data_execucao_sql != data_consulta.strftime('%Y-%m-%d'):
                            continue
                        eventdate = _ajustar_timestamp_iso_para_local(eventdate_raw, 3)
                        updatedate = _ajustar_timestamp_iso_para_local(updatedate_raw, 3)

                        todos_logs.append({
                            'Placa': item.get('TrackedUnitIntegrationCode'),
                            'EventDate': eventdate,
                            'UpdateDate': updatedate,
                            'Ignition': item.get('Ignition'),
                            'Matricula': matricula,
                            'Latitude': item.get('Latitude'),
                            'Longitude': item.get('Longitude'),
                            'Data_Execucao': data_execucao_sql
                        })
            else:
                matricula = dados.get('Driver')
                idevent = dados.get('IdEvent')
                if dados.get('Ignition') == True and matricula is not None and str(matricula).strip() != "" and idevent == 65:
                    eventdate_raw = dados.get('EventDate')
                    updatedate_raw = dados.get('UpdateDate')
                    data_execucao_sql = _derivar_data_execucao_do_evento(eventdate_raw, data_consulta)
                    if data_execucao_sql != data_consulta.strftime('%Y-%m-%d'):
                        continue
                    eventdate = _ajustar_timestamp_iso_para_local(eventdate_raw, 3)
                    updatedate = _ajustar_timestamp_iso_para_local(updatedate_raw, 3)

                    todos_logs.append({
                        'Placa': dados.get('TrackedUnitIntegrationCode'),
                        'EventDate': eventdate,
                        'UpdateDate': updatedate,
                        'Ignition': dados.get('Ignition'),
                        'Matricula': matricula,
                        'Latitude': dados.get('Latitude'),
                        'Longitude': dados.get('Longitude'),
                        'Data_Execucao': data_execucao_sql
                    })
    if not todos_logs:
        return
    df = pd.DataFrame(todos_logs)
    df['EventDate'] = pd.to_datetime(df['EventDate'])
    df['UpdateDate'] = pd.to_datetime(df['UpdateDate'])
    df = df.sort_values(['Placa', 'Matricula', 'EventDate'])
    eventos_filtrados = []
    for (placa, matricula, data_exec), grupo in df.groupby(['Placa', 'Matricula', 'Data_Execucao']):
        subgrupos = _split_by_gap(grupo, 'EventDate', GAP_SECONDS)
        for sub in subgrupos:
            if len(sub) == 1:
                eventos_filtrados.append(sub.iloc[0].to_dict())
            else:
                eventos_filtrados.append(sub.iloc[0].to_dict())   # entrada
                eventos_filtrados.append(sub.iloc[-1].to_dict())  # saída
    conn = get_db_connection()
    cursor = conn.cursor()
    for item in eventos_filtrados:
        cursor.execute("""
            INSERT INTO Veiculo (Placa, EventDate, UpdateDate, Ignition, Matricula, Latitude, Longitude, Data_Execucao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                UpdateDate = VALUES(UpdateDate),
                Ignition = VALUES(Ignition),
                Latitude = VALUES(Latitude),
                Longitude = VALUES(Longitude)
        """, (
            item['Placa'],
            item['EventDate'],
            item['UpdateDate'],
            item['Ignition'],
            item['Matricula'],
            item['Latitude'],
            item['Longitude'],
            item['Data_Execucao']
        ))
    conn.commit()
    cursor.close()
    conn.close()

def garantir_ordem_cronologica_global(entrada_ida, saida_ida, entrada_escola, saida_escola, entrada_volta, saida_volta):
    ei = _to_datetime_or_none(entrada_ida)
    si = _to_datetime_or_none(saida_ida)
    ee = _to_datetime_or_none(entrada_escola)
    se = _to_datetime_or_none(saida_escola)
    ev = _to_datetime_or_none(entrada_volta)
    sv = _to_datetime_or_none(saida_volta)

    if ei and si and si < ei:
        si = ei + timedelta(minutes=30)
    if ee and se and se < ee:
        se = ee + timedelta(minutes=30)
    if ev and sv and sv < ev:
        sv = ev + timedelta(minutes=30)

    if si and ee and ee <= si:
        ee = si + timedelta(minutes=1)
        if se and se < ee:
            se = ee + timedelta(minutes=30)

    if se and ev and ev <= se:
        ev = se + timedelta(minutes=1)
        if sv and sv < ev:
            sv = ev + timedelta(minutes=30)

    return (_fmt(ei), _fmt(si), _fmt(ee), _fmt(se), _fmt(ev), _fmt(sv))

def ancorar_no_presente(data_execucao, ei, si, ee, se, ev, sv):
    base = data_execucao if isinstance(data_execucao, datetime) else _to_datetime_or_none(data_execucao)
    agora = datetime.now()
    if base and base.date() == agora.date():
        dts = [_to_datetime_or_none(x) for x in (ei, si, ee, se, ev, sv)]
        dts = [None if (dt is not None and dt > agora) else dt for dt in dts]
        return tuple(_fmt(x) for x in dts)
    return (ei, si, ee, se, ev, sv)

def corrigir_ordem_em_toda_tabela_aluno(data_execucao: str | None = None):
    """
    Se data_execucao for fornecida ('YYYY-MM-DD'), limita as correções a esse dia.
    Caso contrário, mantém o comportamento anterior (corrige toda a tabela).
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    if data_execucao:
        cursor.execute("""
            UPDATE Aluno
            SET Saida_Ida_Veiculo = DATE_ADD(Entrada_Ida_Veiculo, INTERVAL 30 MINUTE)
            WHERE Entrada_Ida_Veiculo IS NOT NULL
              AND Saida_Ida_Veiculo IS NOT NULL
              AND Saida_Ida_Veiculo < Entrada_Ida_Veiculo
              AND Data_Execucao = %s
        """, (data_execucao,))
        cursor.execute("""
            UPDATE Aluno
            SET Saida_Escola = DATE_ADD(Entrada_Escola, INTERVAL 30 MINUTE)
            WHERE Entrada_Escola IS NOT NULL
              AND Saida_Escola IS NOT NULL
              AND Saida_Escola < Entrada_Escola
              AND Data_Execucao = %s
        """, (data_execucao,))
        cursor.execute("""
            UPDATE Aluno
            SET Saida_Volta_Veiculo = DATE_ADD(Entrada_Volta_Veiculo, INTERVAL 30 MINUTE)
            WHERE Entrada_Volta_Veiculo IS NOT NULL
              AND Saida_Volta_Veiculo IS NOT NULL
              AND Saida_Volta_Veiculo < Entrada_Volta_Veiculo
              AND Data_Execucao = %s
        """, (data_execucao,))
        cursor.execute("""
            UPDATE Aluno
            SET Entrada_Escola = DATE_ADD(Saida_Ida_Veiculo, INTERVAL 1 MINUTE)
            WHERE Saida_Ida_Veiculo IS NOT NULL
              AND Entrada_Escola IS NOT NULL
              AND Entrada_Escola <= Saida_Ida_Veiculo
              AND Data_Execucao = %s
        """, (data_execucao,))
        cursor.execute("""
            UPDATE Aluno
            SET Entrada_Volta_Veiculo = DATE_ADD(Saida_Escola, INTERVAL 1 MINUTE)
            WHERE Saida_Escola IS NOT NULL
              AND Entrada_Volta_Veiculo IS NOT NULL
              AND Entrada_Volta_Veiculo <= Saida_Escola
              AND Data_Execucao = %s
        """, (data_execucao,))
    else:
        cursor.execute("""
            UPDATE Aluno
            SET Saida_Ida_Veiculo = DATE_ADD(Entrada_Ida_Veiculo, INTERVAL 30 MINUTE)
            WHERE Entrada_Ida_Veiculo IS NOT NULL
              AND Saida_Ida_Veiculo IS NOT NULL
              AND Saida_Ida_Veiculo < Entrada_Ida_Veiculo
        """)
        cursor.execute("""
            UPDATE Aluno
            SET Saida_Escola = DATE_ADD(Entrada_Escola, INTERVAL 30 MINUTE)
            WHERE Entrada_Escola IS NOT NULL
              AND Saida_Escola IS NOT NULL
              AND Saida_Escola < Entrada_Escola
        """)
        cursor.execute("""
            UPDATE Aluno
            SET Saida_Volta_Veiculo = DATE_ADD(Entrada_Volta_Veiculo, INTERVAL 30 MINUTE)
            WHERE Entrada_Volta_Veiculo IS NOT NULL
              AND Saida_Volta_Veiculo IS NOT NULL
              AND Saida_Volta_Veiculo < Entrada_Volta_Veiculo
        """)
        cursor.execute("""
            UPDATE Aluno
            SET Entrada_Escola = DATE_ADD(Saida_Ida_Veiculo, INTERVAL 1 MINUTE)
            WHERE Saida_Ida_Veiculo IS NOT NULL
              AND Entrada_Escola IS NOT NULL
              AND Entrada_Escola <= Saida_Ida_Veiculo
        """)
        cursor.execute("""
            UPDATE Aluno
            SET Entrada_Volta_Veiculo = DATE_ADD(Saida_Escola, INTERVAL 1 MINUTE)
            WHERE Saida_Escola IS NOT NULL
              AND Entrada_Volta_Veiculo IS NOT NULL
              AND Entrada_Volta_Veiculo <= Saida_Escola
        """)
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    from datetime import datetime, timedelta
    from authtoken import obter_token
    token_unico = obter_token()
    if not token_unico:
        print("Falha ao obter token. Encerrando.")
    else:
        criar_tabela_escola()
        criar_tabela_veiculo()
        criar_tabela_aluno()
        hoje = datetime.now()
        dia_consulta = hoje
        print(f"Consultando e alimentando banco para o dia: {dia_consulta.strftime('%d/%m/%Y')}")
        consultar_api_escola(dia_consulta, token=token_unico)
        consultar_api_veiculo(dia_consulta, token=token_unico)
        preencher_tabela_aluno(dia_consulta)
        corrigir_ordem_em_toda_tabela_aluno(dia_consulta.strftime('%Y-%m-%d'))
