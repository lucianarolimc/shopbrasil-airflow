"""
Pipeline ShopBrasil: panorama de preços por categoria.

Fluxo:
  ingestao:     buscar_produtos -> validar_produtos                 (linear)
  analise:      agrupar_por_categoria -> calcular_metricas[N]       (fan-out)
                calcular_metricas[N] -> consolidar_metricas         (fan-in)
  persistencia: preparar_tabelas -> salvar_snapshot / salvar_historico
"""

import logging
from datetime import timedelta

import pendulum
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, get_current_context, task, task_group

from shopbrasil import config, sql
from shopbrasil.callbacks import ao_falhar, ao_retentar, ao_ter_sucesso
from shopbrasil.operators.validar_produtos import ValidarProdutosOperator

log = logging.getLogger(__name__)

default_args = {
    "owner": "time-dados",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": ao_falhar,  # toda task avisa quando falhar
}

def _data_referencia(context) -> str:
    """Dia (no horário de Brasília) ao qual a execução se refere."""
    
    data = context.get("logical_date") or context["dag_run"].run_after
    return pendulum.instance(data).in_timezone(config.TZ).date().isoformat()

def _gravar(linhas: list[tuple], comando_sql: str) -> int:
    """Executa um INSERT para várias linhas dentro de uma única transação."""
    
    hook = PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.executemany(comando_sql, linhas)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return len(linhas)

@dag(
    dag_id="shopbrasil_precos_por_categoria",
    description="Coleta produtos da API de catálogo e calcula métricas de preço por categoria",
    schedule="0 6 * * *",  # todo dia às 06:00
    start_date=pendulum.datetime(2026, 11, 1, tz=config.TZ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["shopbrasil", "ecommerce", "precos"],
)
def pipeline_precos_por_categoria():
    """Monta a DAG: ingestão -> análise -> persistência."""

    # ------------------------------------------------------------------ INGESTÃO
    @task_group(group_id="ingestao")
    def ingestao():
        """Busca os produtos na API e valida o schema."""

        @task(
            task_id="buscar_produtos",
            retries=5,
            retry_delay=timedelta(seconds=30),  # valor original é 30
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=10),
            on_failure_callback=ao_falhar,
            on_retry_callback=ao_retentar,
            on_success_callback=ao_ter_sucesso,
        )
        def buscar_produtos() -> list[dict]:
            """Chama a API de catálogo e devolve a lista de produtos."""
            
            url = f"{config.API_BASE_URL}/products"
            try:
                resposta = requests.get(url, timeout=config.API_TIMEOUT_SEGUNDOS)
                resposta.raise_for_status()  # 4xx/5xx viram exceção
                produtos = resposta.json()
                if not produtos:
                    raise ValueError("A API retornou uma lista vazia de produtos.")
            except (requests.RequestException, ValueError) as erro:
                log.error("Falha ao consultar %s: %s", url, erro)
                raise  # relança para o Airflow acionar o retry
            log.info("%d produtos recebidos da API.", len(produtos))
            return produtos

        produtos = buscar_produtos()
        validar = ValidarProdutosOperator(task_id="validar_produtos", produtos=produtos)
        return validar.output

    # ------------------------------------------------------------------ ANÁLISE
    @task_group(group_id="analise")
    def analise(produtos):
        """Agrupa por categoria, calcula as métricas em paralelo e consolida."""

        @task
        def agrupar_por_categoria(produtos: list[dict]) -> list[dict]:
            """Separa os preços por categoria: um item da lista para cada categoria."""
            grupos: dict[str, list[float]] = {}
            for produto in produtos:
                grupos.setdefault(produto["categoria"], []).append(produto["preco"])
            log.info("Categorias encontradas: %s", sorted(grupos))
            return [{"categoria": cat, "precos": precos} for cat, precos in sorted(grupos.items())]

        @task(pool=config.POOL_NAME, map_index_template="{{ categoria_atual }}")
        def calcular_metricas(grupo: dict) -> dict:
            """Calcula quantidade, preço médio, mínimo e máximo de uma categoria."""
            # nome legível para cada instância mapeada na interface
            get_current_context()["categoria_atual"] = grupo["categoria"]
            precos = grupo["precos"]
            return {
                "categoria": grupo["categoria"],
                "qtd_produtos": len(precos),
                "preco_medio": round(sum(precos) / len(precos), 2),
                "preco_min": round(min(precos), 2),
                "preco_max": round(max(precos), 2),
            }

        @task
        def consolidar_metricas(metricas) -> list[dict]:
            """Junta as métricas de todas as categorias numa lista ordenada."""
            consolidado = sorted(metricas, key=lambda m: m["categoria"])
            for m in consolidado:
                log.info("%s", m)
            return consolidado

        grupos = agrupar_por_categoria(produtos)
        metricas = calcular_metricas.expand(grupo=grupos)  # FAN-OUT
        return consolidar_metricas(metricas)               # FAN-IN

    # ------------------------------------------------------------------ PERSISTÊNCIA
    @task_group(group_id="persistencia")
    def persistencia(metricas):
        """Cria as tabelas (se preciso) e grava snapshot e histórico."""

        @task
        def preparar_tabelas():
            """Cria as tabelas no banco analítico, se ainda não existirem."""
            PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID).run(sql.CRIAR_TABELAS)

        @task
        def salvar_snapshot(metricas: list[dict]) -> int:
            """Grava a foto do dia com upsert (reprocessar não duplica)."""
            data_ref = _data_referencia(get_current_context())
            linhas = [
                (data_ref, m["categoria"], m["qtd_produtos"], m["preco_medio"], m["preco_min"], m["preco_max"])
                for m in metricas
            ]
            total = _gravar(linhas, sql.UPSERT_SNAPSHOT)
            log.info("Snapshot de %s: %d categorias gravadas (upsert).", data_ref, total)
            return total

        @task
        def salvar_historico(metricas: list[dict]) -> int:
            """Acrescenta as métricas desta execução no histórico (append)."""
            contexto = get_current_context()
            data_ref = _data_referencia(contexto)
            linhas = [
                (data_ref, contexto["run_id"], m["categoria"], m["qtd_produtos"],
                 m["preco_medio"], m["preco_min"], m["preco_max"])
                for m in metricas
            ]
            total = _gravar(linhas, sql.INSERIR_HISTORICO)
            log.info("Histórico: %d linhas adicionadas (append).", total)
            return total

        tabelas = preparar_tabelas()
        tabelas >> [salvar_snapshot(metricas), salvar_historico(metricas)]

    # ------------------------------------------------------------------ MONTAGEM
    produtos_validos = ingestao()
    metricas = analise(produtos_validos)
    persistencia(metricas)


pipeline_precos_por_categoria()