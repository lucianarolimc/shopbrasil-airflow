"""Callbacks de ciclo de vida das tasks (sucesso, retry e falha)."""
import logging

log = logging.getLogger(__name__)


def _identificar(context) -> str:
    """Monta um texto com a DAG, a task e o número da tentativa."""
    
    ti = context["ti"]
    return f"dag={ti.dag_id} task={ti.task_id} tentativa={ti.try_number}"


def _erro(context) -> str:
    """Devolve a mensagem de erro da task, ou um aviso para olhar o log."""
    
    return str(context.get("exception") or "detalhes no log da task")


def ao_ter_sucesso(context):
    """Chamada pelo Airflow quando a task termina com sucesso."""
    
    log.info("[SUCESSO] %s concluída.", _identificar(context))


def ao_retentar(context):
    """Chamada pelo Airflow quando a task falha, mas ainda vai tentar de novo."""
    
    log.warning("[RETRY] %s falhou e será executada novamente. Erro: %s",
                _identificar(context), _erro(context))


def ao_falhar(context):
    """Chamada pelo Airflow quando a task falha de vez; simula o envio de um alerta."""
    
    mensagem = (
        "Pipeline ShopBrasil FALHOU!\n"
        f"{_identificar(context)}\n"
        f"Erro: {_erro(context)}"
    )
    log.error("[ALERTA SIMULADO] Enviando para o canal #dados-alertas:\n%s", mensagem)
    # Em produção, aqui entraria algo como:
    # requests.post(SLACK_WEBHOOK_URL, json={"text": mensagem}, timeout=10)