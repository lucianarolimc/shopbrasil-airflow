"""Comandos SQL usados pelo pipeline"""
from shopbrasil.config import TABELA_HISTORICO, TABELA_SNAPSHOT

CRIAR_TABELAS = f"""
CREATE TABLE IF NOT EXISTS {TABELA_SNAPSHOT} (
    data_referencia DATE          NOT NULL,
    categoria       TEXT          NOT NULL,
    qtd_produtos    INTEGER       NOT NULL,
    preco_medio     NUMERIC(12,2) NOT NULL,
    preco_min       NUMERIC(12,2) NOT NULL,
    preco_max       NUMERIC(12,2) NOT NULL,
    atualizado_em   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (data_referencia, categoria)
);

CREATE TABLE IF NOT EXISTS {TABELA_HISTORICO} (
    id              BIGSERIAL     PRIMARY KEY,
    data_referencia DATE          NOT NULL,
    run_id          TEXT          NOT NULL,
    categoria       TEXT          NOT NULL,
    qtd_produtos    INTEGER       NOT NULL,
    preco_medio     NUMERIC(12,2) NOT NULL,
    preco_min       NUMERIC(12,2) NOT NULL,
    preco_max       NUMERIC(12,2) NOT NULL,
    coletado_em     TIMESTAMPTZ   NOT NULL DEFAULT now()
);
"""

# UPSERT: insere; se a chave (data, categoria) já existir, atualiza.
UPSERT_SNAPSHOT = f"""
INSERT INTO {TABELA_SNAPSHOT}
    (data_referencia, categoria, qtd_produtos, preco_medio, preco_min, preco_max, atualizado_em)
VALUES (%s, %s, %s, %s, %s, %s, now())
ON CONFLICT (data_referencia, categoria) DO UPDATE SET
    qtd_produtos  = EXCLUDED.qtd_produtos,
    preco_medio   = EXCLUDED.preco_medio,
    preco_min     = EXCLUDED.preco_min,
    preco_max     = EXCLUDED.preco_max,
    atualizado_em = now();
"""

# APPEND: sempre insere uma nova linha (histórico de evolução de preços).
INSERIR_HISTORICO = f"""
INSERT INTO {TABELA_HISTORICO}
    (data_referencia, run_id, categoria, qtd_produtos, preco_medio, preco_min, preco_max)
VALUES (%s, %s, %s, %s, %s, %s, %s);
"""
