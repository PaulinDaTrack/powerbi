import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
import time
from apscheduler.schedulers.blocking import BlockingScheduler

from grid import processar_grid
from ultima_execucao import atualizar_ultima_execucao
from routeviolation import routeviolation, verificar_violações_por_velocidade, refresh_mv
from remover_rotas_canceladas import remover_rotas_canceladas

def log_execution_time(func):
    def wrapper():
        logging.info(f"Iniciando job: {func.__name__}")
        try:
            start_time = time.time()
            func()
            elapsed_time = time.time() - start_time
            logging.info(f"Job {func.__name__} finalizado em {elapsed_time:.2f} segundos.")
        except Exception as e:
            logging.exception(f"Erro crítico no job {func.__name__}: {e}")
    return wrapper

def routeviolation_completo():
    logging.info("Iniciando job: routeviolation_completo")
    try:
        from authtoken import obter_token
        token = obter_token()
        if token:
            routeviolation(token)
            verificar_violações_por_velocidade(token)
        else:
            logging.error("Não foi possível obter o token.")
        logging.info("Job routeviolation_completo finalizado.")
    except Exception as e:
        logging.exception(f"Erro crítico no job routeviolation_completo: {e}")

def refresh_mv_job():
    logging.info("Iniciando job: refresh_mv_job")
    try:
        start_time = time.time()
        refresh_mv()
        elapsed_time = time.time() - start_time
        logging.info(f"Job refresh_mv_job finalizado em {elapsed_time:.2f} segundos.")
    except Exception as e:
        logging.exception(f"Erro crítico no job refresh_mv_job: {e}")

def tags_job():
    logging.info("Iniciando job: tags_job")
    try:
        from datetime import datetime
        from authtoken import obter_token
        from tags import (
            criar_tabela_escola,
            criar_tabela_veiculo,
            criar_tabela_aluno,
            consultar_api_escola,
            consultar_api_veiculo,
            preencher_tabela_aluno,
            corrigir_ordem_em_toda_tabela_aluno,
        )
        data_ref = datetime.now()
        token = obter_token()
        if not token:
            logging.error("tags_job: falha ao obter token.")
            return
        criar_tabela_escola()
        criar_tabela_veiculo()
        criar_tabela_aluno()
        consultar_api_escola(data_ref, token=token)
        consultar_api_veiculo(data_ref, token=token)
        preencher_tabela_aluno(data_ref)
        corrigir_ordem_em_toda_tabela_aluno(data_ref.strftime('%Y-%m-%d'))
        logging.info("tags_job finalizado com sucesso.")
    except Exception as e:
        logging.exception(f"Erro no tags_job: {e}")

scheduler = BlockingScheduler()
scheduler.add_job(
    func=log_execution_time(processar_grid),
    trigger="interval",
    minutes=10,
    max_instances=1,
    coalesce=True,
)
scheduler.add_job(
    func=log_execution_time(atualizar_ultima_execucao),
    trigger="interval",
    minutes=10,
    max_instances=1,
    coalesce=True,
)
scheduler.add_job(
    func=log_execution_time(routeviolation_completo),
    trigger="interval",
    minutes=10,
    max_instances=1,
    coalesce=True,
)
scheduler.add_job(
    func=refresh_mv_job,
    trigger="interval",
    minutes=60,
    max_instances=1,
    coalesce=True,
)
scheduler.add_job(
    func=log_execution_time(tags_job),
    trigger="cron",
    minute=0,
    hour="*/2",
    max_instances=1,
    coalesce=True,
)
scheduler.add_job(
    func=log_execution_time(remover_rotas_canceladas),
    trigger="cron",
    hour="19",
    minute="0",
    max_instances=1,
    coalesce=True,
)
try:
    scheduler.start()
    logging.info("Agendador iniciado com sucesso.")
except Exception as e:
    logging.error(f"Erro ao iniciar o agendador: {e}")