"""Operador customizado que valida o schema dos produtos vindos da API."""
from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator

# campo -> tipo(s) aceito(s)
SCHEMA_PRODUTO = {
    "id": int,
    "title": str,
    "price": (int, float),
    "category": str,
}


class ValidarProdutosOperator(BaseOperator):
    """Valida cada produto e devolve só os válidos, já enxutos.

    - Produto sem campo obrigatório, com tipo errado ou preço negativo é descartado.
    - Se nenhum produto for válido, a task falha.
    - Devolve apenas id, categoria e preço (XCom pequeno).
    """

    # Permite receber o retorno de outra task (XComArg) neste parâmetro
    template_fields = ("produtos",)

    def __init__(self, produtos, **kwargs):
        """Guarda a lista de produtos que será validada."""
        super().__init__(**kwargs)
        self.produtos = produtos

    @staticmethod
    def _erros(produto: dict) -> list[str]:
        """Confere um produto e devolve a lista de problemas (vazia = válido)."""
        erros = []
        for campo, tipo in SCHEMA_PRODUTO.items():
            if campo not in produto:
                erros.append(f"campo ausente: {campo}")
            elif not isinstance(produto[campo], tipo):
                erros.append(f"tipo inválido em {campo}: {type(produto[campo]).__name__}")
        if not erros and produto["price"] < 0:
            erros.append("preço negativo")
        return erros

    def execute(self, context):
        """Executado pelo Airflow: filtra os produtos válidos e devolve a versão enxuta."""
        validos, descartados = [], 0
        for produto in self.produtos:
            erros = self._erros(produto)
            if erros:
                descartados += 1
                self.log.warning("Produto descartado (%s): %s", produto.get("id"), erros)
                continue
            validos.append(
                {"id": produto["id"], "categoria": produto["category"], "preco": float(produto["price"])}
            )

        self.log.info("Validação: %d válidos, %d descartados.", len(validos), descartados)
        if not validos:
            raise AirflowException("Nenhum produto válido recebido da API.")
        return validos