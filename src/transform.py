"""
Sprint 2 — Saneamento e Qualidade (transform.py)
=================================================
Lê os dados da camada RAW, aplica limpeza via DuckDB (Push Down)
e valida o schema com Pandera antes de salvar na camada TRUSTED.

REGRAS:
  - DuckDB faz o trabalho pesado (filtros, agregações, type casting).
  - Pandera valida o contrato de dados — se falhar, o script PARA.
  - Saída: data/trusted/dataset.parquet
"""

from pathlib import Path

import duckdb
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check

# ── Caminhos ──────────────────────────────────────────────────────────────────
RAW_DIR = Path("data/raw")
TRUSTED_DIR = Path("data/trusted")
TRUSTED_DIR.mkdir(parents=True, exist_ok=True)

# ── 1. CONTRATO DE DADOS (Pandera Schema) ─────────────────────────────────────
# TODO: Ajuste as colunas e checks conforme o seu dataset

schema = DataFrameSchema(
    {
        "id": Column(int, Check.greater_than(0), nullable=False),
        "valor": Column(float, Check.greater_than_or_equal_to(0.0), nullable=False),
        "data": Column(str, nullable=False),
        # Adicione mais colunas do seu dataset aqui
    },
    coerce=True,   # Tenta converter os tipos automaticamente
    strict=False,  # Permite colunas extras além das declaradas
)

# ── 2. LIMPEZA VIA DUCKDB (Push Down Computation) ─────────────────────────────

def load_and_clean() -> pd.DataFrame:
    """
    Usa DuckDB para ler todos os JSONs da camada raw e aplicar
    transformações pesadas (filtros, type cast, dedup) em SQL.
    """
    print("[TRANSFORM] Lendo dados raw com DuckDB...")

    con = duckdb.connect()

    # Push Down: SQL faz o trabalho pesado antes de entrar no Pandas
    df = con.execute("""
        WITH raw_data AS (
            -- Lê todos os arquivos JSON da pasta raw
            SELECT *
            FROM read_json_auto('data/raw/*.json', ignore_errors=true)
        ),
        deduplicado AS (
            -- Remove duplicatas mantendo o registro mais recente
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY id ORDER BY data DESC) AS rn
            FROM raw_data
        )
        SELECT
            -- TODO: Ajuste as colunas conforme o seu dataset
            CAST(id AS INTEGER)       AS id,
            CAST(valor AS DOUBLE)     AS valor,
            CAST(data AS VARCHAR)     AS data
            -- Adicione mais transformações aqui
        FROM deduplicado
        WHERE rn = 1
          AND id IS NOT NULL
          AND valor >= 0
    """).df()

    registros_raw = len(df)
    print(f"[TRANSFORM] {registros_raw} registros após limpeza SQL.")
    con.close()
    return df


# ── 3. LIMPEZA FINA VIA PANDAS (Fine Tuning) ──────────────────────────────────

def fine_tune(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica refinamentos que são mais naturais em Pandas:
    imputação de nulos, engenharia de colunas simples, etc.
    """
    antes = len(df)

    # Exemplo: remover linhas onde 'valor' é nulo após o cast
    df = df.dropna(subset=["valor"])

    # TODO: Adicione sua lógica de limpeza fina aqui
    # df['valor'] = df['valor'].fillna(df['valor'].median())

    depois = len(df)
    descartados = antes - depois
    print(f"[TRANSFORM] Fine-tune: {descartados} registros descartados (nulos/inválidos).")
    return df


# ── 4. VALIDAÇÃO DO CONTRATO (Pandera) ────────────────────────────────────────

def validate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Valida o DataFrame contra o schema definido.
    Se o contrato for violado, levanta uma exceção e o pipeline PARA.
    """
    print("[TRANSFORM] Validando contrato de dados com Pandera...")
    try:
        df_validado = schema.validate(df, lazy=True)
        print("[TRANSFORM] ✅ Contrato validado com sucesso.")
        return df_validado
    except pa.errors.SchemaErrors as errs:
        print("[TRANSFORM] ❌ FALHA NA VALIDAÇÃO DO SCHEMA:")
        print(errs.failure_cases.to_string())
        raise SystemExit(1)


# ── 5. SALVAR NA CAMADA TRUSTED ───────────────────────────────────────────────

def save_trusted(df: pd.DataFrame, name: str = "dataset") -> Path:
    output_path = TRUSTED_DIR / f"{name}.parquet"
    df.to_parquet(output_path, index=False, engine="pyarrow")
    print(f"[TRANSFORM] Dados confiáveis salvos em: {output_path} ({len(df)} registros).")
    return output_path


# ── Ponto de Entrada ──────────────────────────────────────────────────────────

def main():
    df = load_and_clean()
    df = fine_tune(df)
    df = validate(df)
    save_trusted(df, name="dataset")
    print("[TRANSFORM] ✅ Sprint 2 concluído.")


if __name__ == "__main__":
    main()
