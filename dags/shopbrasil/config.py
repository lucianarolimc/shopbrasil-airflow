"""Configurações centrais do pipeline"""
import pendulum

# fuso horário oficial do pipeline: horário de Brasília
TZ = pendulum.timezone("America/Sao_Paulo")

# fonte de dados (API de catálogo)
API_BASE_URL = "https://fakestoreapi.com"
API_TIMEOUT_SEGUNDOS = 30

# recursos do airflow
POSTGRES_CONN_ID = "postgres_dw"   # connection criada via variável de ambiente
POOL_NAME = "ecommerce_pool"        # pool com 2 slots

# tabelas de destino
TABELA_SNAPSHOT = "metricas_categoria"
TABELA_HISTORICO = "metricas_categoria_historico"

