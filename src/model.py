"""
Sprint 3 — Modelagem & Engenharia de Features (model.py)
=========================================================
Lê a camada Trusted e constrói duas visões analíticas no DuckDB:
  1. Star Schema (BI) — Fato + Dimensões para relatórios e KPIs.
  2. Feature Store (ML) — Flat table com variáveis explicativas.

REGRAS:
  - Todo processamento pesado é feito via SQL no DuckDB.
  - Pandas apenas para feature engineering fina (shifts, rolling).
  - Saídas: data/mart/ e data/feat/
"""

from pathlib import Path

import duckdb
import pandas as pd

# ── Caminhos ──────────────────────────────────────────────────────────────────
TRUSTED_PATH = Path("data/trusted/dataset.parquet")
MART_DIR = Path("data/mart")
FEAT_DIR = Path("data/feat")
DB_PATH = Path("data/analytics.duckdb")

for d in [MART_DIR, FEAT_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── 1. STAR SCHEMA (BI) ───────────────────────────────────────────────────────

def build_star_schema(con: duckdb.DuckDBPyConnection) -> None:
    """
    Cria as tabelas Fato e Dimensão para modelagem BI.
    TODO: Adapte as queries ao schema do seu dataset.
    """
    print("[MODEL] Construindo Star Schema...")

    # Carrega a camada trusted no DuckDB
    con.execute(f"CREATE OR REPLACE TABLE trusted AS SELECT * FROM read_parquet('{TRUSTED_PATH}')")

    # ── Tabela Dimensão: Exemplo (dim_tempo) ──────────────────────────────────
    con.execute("""
        CREATE OR REPLACE TABLE dim_tempo AS
        SELECT DISTINCT
            CAST(data AS DATE)                          AS data,
            EXTRACT(year  FROM CAST(data AS DATE))      AS ano,
            EXTRACT(month FROM CAST(data AS DATE))      AS mes,
            EXTRACT(quarter FROM CAST(data AS DATE))    AS trimestre,
            DAYOFWEEK(CAST(data AS DATE))               AS dia_semana
        FROM trusted
    """)

    # TODO: Crie mais dimensões conforme o seu dataset (dim_produto, dim_regiao...)

    # ── Tabela Fato ────────────────────────────────────────────────────────────
    con.execute("""
        CREATE OR REPLACE TABLE fato_principal AS
        SELECT
            t.id,
            t.valor,
            CAST(t.data AS DATE) AS data
            -- TODO: Adicione chaves estrangeiras para as dimensões
        FROM trusted t
    """)

    # Exporta para Parquet
    con.execute(f"COPY fato_principal TO '{MART_DIR}/fato_principal.parquet' (FORMAT PARQUET)")
    con.execute(f"COPY dim_tempo TO '{MART_DIR}/dim_tempo.parquet' (FORMAT PARQUET)")

    print("[MODEL] Star Schema salvo em data/mart/")


# ── 2. FEATURE STORE (ML) ─────────────────────────────────────────────────────

def build_feature_store(con: duckdb.DuckDBPyConnection) -> None:
    """
    Cria a Feature Store: uma flat table com variáveis para ML.
    Inclui features temporais (lag, rolling mean) calculadas via Pandas.
    """
    print("[MODEL] Construindo Feature Store...")

    # SQL faz o trabalho pesado: ordenação, agrupamentos, métricas base
    df = con.execute("""
        SELECT
            id,
            CAST(data AS DATE)  AS data,
            valor,
            -- Estatística descritiva com Window Functions
            AVG(valor) OVER (PARTITION BY id ORDER BY data
                             ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS media_movel_7d,
            RANK() OVER (ORDER BY valor DESC)                          AS rank_valor
        FROM trusted
        ORDER BY id, data
    """).df()

    # Pandas para features que são mais naturais em Python
    # TODO: Ajuste a coluna de agrupamento conforme seu dataset
    df = df.sort_values(["id", "data"])
    df["lag_1d"] = df.groupby("id")["valor"].shift(1)
    df["lag_7d"] = df.groupby("id")["valor"].shift(7)

    # Remove linhas sem features completas (NaN dos lags iniciais)
    df_features = df.dropna(subset=["lag_1d"])

    feat_path = FEAT_DIR / "features.parquet"
    df_features.to_parquet(feat_path, index=False)
    print(f"[MODEL] Feature Store salva em: {feat_path} ({len(df_features)} registros).")


# ── 3. DICIONÁRIO DE DADOS ────────────────────────────────────────────────────

def generate_data_dict(con: duckdb.DuckDBPyConnection) -> None:
    """Gera um dicionário de dados simples a partir das tabelas criadas."""
    print("[MODEL] Gerando dicionário de dados...")
    tables = ["trusted", "fato_principal", "dim_tempo"]
    lines = ["# Dicionário de Dados\n"]
    for table in tables:
        try:
            info = con.execute(f"DESCRIBE {table}").df()
            lines.append(f"\n## Tabela: `{table}`\n")
            lines.append(info[["column_name", "column_type"]].to_markdown(index=False))
            lines.append("\n")
        except Exception:
            pass
    with open("reports/data_dictionary.md", "w") as f:
        f.writelines(lines)
    print("[MODEL] Dicionário salvo em reports/data_dictionary.md")


# ── Ponto de Entrada ──────────────────────────────────────────────────────────

def main():
    Path("reports").mkdir(exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    build_star_schema(con)
    build_feature_store(con)
    generate_data_dict(con)
    con.close()
    print("[MODEL] ✅ Sprint 3 concluído.")


if __name__ == "__main__":
    main()
