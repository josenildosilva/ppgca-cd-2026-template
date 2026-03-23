"""
Sprint 2 — Qualidade & Saneamento (transform_advanced.py)
==========================================================
Variante avançada: pipeline Raw -> Trusted com rastreabilidade completa.
Baseada no padrão de produção da disciplina (trusted.py de referência).

Use este arquivo quando quiser:
  - Logging estruturado com loguru (em vez de print)
  - Rastreabilidade detalhada: motivo e contagem de cada descarte
  - Guardrail de qualidade mínima (LIMITE_DESCARTE_PCT)
  - Exportação particionada via DuckDB COPY TO

Use transform_basic.py se você está iniciando o Sprint 2
e quer a estrutura mais simples para adaptar.

Conceitos demonstrados:
  - Push Down Computation: filtros pesados no DuckDB, não no Pandas
  - Data Contracts com Pandera (validação bloqueante)
  - Engenharia Defensiva: script para se dado estiver sujo
  - Logs de rastreabilidade: quantos registros foram descartados e por quê
  - Exportação para Parquet particionado (formato colunar eficiente)

COMO ADAPTAR ESTE ARQUIVO
==========================
Você só precisa editar três seções, marcadas com # <-- ADAPTE:

  ADAPTE 1 — Configuração      (~linha 45):  paths, glob, query SQL
  ADAPTE 2 — Contrato de dados (~linha 85):  schema Pandera (colunas e checks)
  ADAPTE 3 — Lógica de limpeza (~linha 150): regras de negócio no Pandas

Não mexa no restante do arquivo (zona fixa de infraestrutura).

Uso:
    uv run python src/transform_advanced.py

Pré-requisito:
    uv run python src/ingest.py  (camada Raw precisa existir)
"""

import os
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import pandera.pandas as pa
from dotenv import load_dotenv
from loguru import logger
from pandera.pandas import Column, DataFrameSchema, Check

load_dotenv()


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  ADAPTE 1 — Configuração                                                    ║
# ║  Ajuste os caminhos, o glob de leitura, a query SQL e o limite de descarte. ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

# Caminhos das camadas (lidos do .env se existirem, senão usa o padrão abaixo)
RAW_DIR     = Path(os.getenv("RAW_DIR",     "data/raw"))
TRUSTED_DIR = Path(os.getenv("TRUSTED_DIR", "data/trusted"))

# Guardrail: se mais que X% dos registros forem descartados, o pipeline PARA.
# Ajuste conforme a tolerância do seu domínio (0.10 = 10%, 0.30 = 30%).
LIMITE_DESCARTE_PCT = 0.30

# Padrão de leitura dos arquivos raw (glob).
# Exemplos: "*.json", "vendas_*.json", "**/*.csv"
GLOB_RAW = "*.json"

# Query de leitura e transformação inicial (Push Down Computation).
# Coloque aqui os filtros e CASTs que o DuckDB pode executar antes do Pandas.
# Regra: projete aqui as colunas de partição se for usar PARTITION_BY no Step 5.
QUERY_RAW = f"""
    SELECT
        id_pedido,
        CAST(ano AS INTEGER)  AS ano,
        CAST(mes AS INTEGER)  AS mes,
        categoria,
        regiao,
        CAST(valor AS DOUBLE) AS valor
    FROM read_json_auto('{RAW_DIR}/{GLOB_RAW}', union_by_name=True)
    WHERE ano BETWEEN 2020 AND 2025
"""

# Colunas usadas como chave de partição no COPY TO (devem estar na QUERY_RAW)
COLUNAS_PARTICAO = ["ano", "mes"]


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  ADAPTE 2 — Contrato de dados (Pandera Schema)                              ║
# ║  Declare as colunas obrigatórias, seus tipos e checks de domínio.           ║
# ║                                                                             ║
# ║  Cada Column() aceita:                                                      ║
# ║    - tipo Python: str, int, float                                           ║
# ║    - check opcional: Check.isin([...]), Check.greater_than(0), etc.         ║
# ║    - nullable=False para colunas obrigatórias                               ║
# ║                                                                             ║
# ║  strict=False  -> permite colunas extras vindas da API                      ║
# ║  coerce=True   -> tenta converter tipos automaticamente                     ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

SCHEMA_RAW = DataFrameSchema(
    columns={
        "id_pedido": Column(str,   nullable=False),
        "ano":       Column(int,   Check.isin(list(range(2020, 2026))), nullable=False),
        "mes":       Column(int,   Check.isin(list(range(1, 13))),      nullable=False),
        "categoria": Column(str,   nullable=False),
        "regiao":    Column(str,   nullable=True),
        "valor":     Column(float, Check.greater_than_or_equal_to(0),   nullable=True),
    },
    checks=[
        Check(lambda df: len(df) > 0, error="Dataset vazio — verifique a ingestão"),
    ],
    strict=False,
    coerce=True,
)


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  ADAPTE 3 — Lógica de limpeza (Pandas Fine Tuning)                         ║
# ║  Implemente aqui as regras de negócio específicas do seu dataset.           ║
# ║                                                                             ║
# ║  Use sempre registrar_descarte() para documentar cada remoção.             ║
# ║  O relatório gerado ao final é o entregável do Sprint 2.                   ║
# ║                                                                             ║
# ║  Padrão de uso:                                                             ║
# ║      n_antes = len(df)                                                      ║
# ║      df = df[<condição de retenção>]                                        ║
# ║      registrar_descarte("motivo legível", n_antes, len(df))                ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def _aplicar_limpeza(df: pd.DataFrame, registrar_descarte) -> pd.DataFrame:
    """Regras de limpeza e tipagem específicas do seu dataset."""

    # ── Remover duplicatas ────────────────────────────────────────────────────
    n_antes = len(df)
    df = df.drop_duplicates(subset=["id_pedido"])
    registrar_descarte("duplicata por id_pedido", n_antes, len(df))

    # ── Descartar registros sem categoria ─────────────────────────────────────
    n_antes = len(df)
    df = df.dropna(subset=["categoria"])
    registrar_descarte("categoria nula", n_antes, len(df))

    # ── Descartar valores negativos ───────────────────────────────────────────
    n_antes = len(df)
    df = df[df["valor"] >= 0]
    registrar_descarte("valor negativo (corrompido)", n_antes, len(df))

    # ── Normalizar strings ────────────────────────────────────────────────────
    df["categoria"] = df["categoria"].str.strip().str.title()
    df["regiao"]    = df["regiao"].fillna("Não informada").str.strip().str.upper()

    return df


# ══════════════════════════════════════════════════════════════════════════════
#  ZONA FIXA — infraestrutura do pipeline
#  Não é necessário alterar nada abaixo desta linha.
# ══════════════════════════════════════════════════════════════════════════════

def _carregar_raw() -> pd.DataFrame:
    """
    Executa QUERY_RAW via DuckDB e retorna o DataFrame resultante.

    Push Down Computation: filtros e CASTs acontecem no motor SQL,
    antes de qualquer byte chegar ao Pandas.
    """
    arquivos = list(RAW_DIR.glob(GLOB_RAW))
    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum arquivo raw encontrado em {RAW_DIR} com padrão '{GLOB_RAW}'. "
            "Execute primeiro: uv run python src/ingest.py"
        )
    logger.info(f"Arquivos raw encontrados: {len(arquivos)}")

    df = duckdb.connect().execute(QUERY_RAW).df()
    logger.info(f"Registros carregados do Raw: {len(df)}")
    return df


def _validar_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Valida o DataFrame contra SCHEMA_RAW. Para o pipeline em caso de falha."""
    logger.info("Validando schema com Pandera...")
    try:
        df_validado = SCHEMA_RAW.validate(df)
        logger.success("Schema válido — todos os campos obrigatórios presentes")
        return df_validado
    except pa.errors.SchemaError as e:
        logger.error("FALHA NA VALIDAÇÃO DO DATA CONTRACT")
        logger.error(f"  Detalhe: {e.args[0]}")
        logger.error("  Corrija o arquivo raw ou atualize o schema antes de continuar.")
        raise SystemExit(1)


def _limpar_e_tipar(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Orquestra a limpeza delegando a lógica ao _aplicar_limpeza() (ADAPTE 3).
    Registra cada descarte e compila o relatório de qualidade.
    """
    total_inicial = len(df)
    relatorio = {"total_inicial": total_inicial, "descartes": []}

    def registrar_descarte(motivo: str, n_antes: int, n_depois: int) -> None:
        descartados = n_antes - n_depois
        pct = descartados / total_inicial * 100
        if descartados > 0:
            relatorio["descartes"].append({
                "motivo": motivo,
                "registros_descartados": descartados,
                "percentual": round(pct, 2),
            })
            logger.warning(f"  Descarte [{motivo}]: {descartados} registros ({pct:.1f}%)")

    df = _aplicar_limpeza(df, registrar_descarte)

    relatorio["total_final"]      = len(df)
    relatorio["total_descartado"] = total_inicial - len(df)
    relatorio["pct_descartado"]   = round(
        relatorio["total_descartado"] / total_inicial * 100, 2
    )
    return df, relatorio


def _verificar_qualidade_minima(relatorio: dict) -> None:
    """Para o pipeline se a taxa de descarte ultrapassar LIMITE_DESCARTE_PCT."""
    pct = relatorio["pct_descartado"] / 100
    if pct > LIMITE_DESCARTE_PCT:
        logger.error("QUALIDADE INSUFICIENTE — pipeline interrompido")
        logger.error(
            f"  {relatorio['pct_descartado']:.1f}% dos registros descartados "
            f"(limite: {LIMITE_DESCARTE_PCT * 100:.0f}%)"
        )
        logger.error("  Investigue os dados raw antes de continuar.")
        raise SystemExit(1)

    logger.success(
        f"Qualidade aprovada: {relatorio['total_final']} registros válidos "
        f"({relatorio['pct_descartado']:.1f}% descartados)"
    )


def _exportar_trusted(df: pd.DataFrame) -> Path:
    """
    Salva camada Trusted em Parquet particionado (via DuckDB COPY TO).

    Por que Parquet?
      - Formato colunar: leitura 10-50x mais rápida para queries analíticas
      - Compressão automática: tipicamente 5-10x menor que CSV
      - Schema embutido: tipos de dados preservados sem ambiguidade

    Por que particionar?
      - Queries com WHERE nas colunas de partição leem só o subdiretório
        correspondente (partition pruning). Sem partição, leria tudo.
    """
    TRUSTED_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.register("df_trusted", df)
    particoes = ", ".join(COLUNAS_PARTICAO)

    con.execute(f"""
        COPY (SELECT * FROM df_trusted)
        TO '{TRUSTED_DIR}'
        (
            FORMAT      PARQUET,
            PARTITION_BY ({particoes}),
            COMPRESSION 'zstd',
            OVERWRITE_OR_IGNORE true
        )
    """)

    arquivos = list(TRUSTED_DIR.rglob("*.parquet"))
    total_mb = sum(f.stat().st_size for f in arquivos) / 1_048_576
    logger.success(
        f"Trusted salvo: {TRUSTED_DIR} "
        f"({len(arquivos)} arquivo(s) Parquet, {total_mb:.2f} MB, {len(df)} linhas)"
    )
    return TRUSTED_DIR


def _imprimir_relatorio(relatorio: dict) -> None:
    """Exibe resumo do processo de limpeza no terminal."""
    logger.info("Relatório de Qualidade de Dados")
    logger.info(f"  Registros iniciais : {relatorio['total_inicial']}")
    logger.info(f"  Registros finais   : {relatorio['total_final']}")
    logger.info(
        f"  Total descartados  : {relatorio['total_descartado']} "
        f"({relatorio['pct_descartado']:.1f}%)"
    )
    if relatorio["descartes"]:
        logger.info("  Detalhamento dos descartes:")
        for d in relatorio["descartes"]:
            logger.info(
                f"    - {d['motivo']}: "
                f"{d['registros_descartados']} registros ({d['percentual']:.1f}%)"
            )
    else:
        logger.info("  Nenhum descarte realizado — dados limpos!")


def _exportar_relatorio_md(relatorio: dict, destino: Path = Path("reports")) -> Path:
    """
    Gera reports/quality_report.md — entregável do Sprint 2.

    Estrutura:
      1. Resumo executivo (totais e percentual de descarte)
      2. Tabela de descartes por motivo
      3. Veredicto de qualidade (aprovado / reprovado)
      4. Seção de decisões de limpeza (preenchida manualmente pelo aluno)
    """
    destino.mkdir(parents=True, exist_ok=True)
    caminho = destino / "quality_report.md"

    aprovado  = relatorio["pct_descartado"] / 100 <= LIMITE_DESCARTE_PCT
    veredicto = "APROVADO" if aprovado else "REPROVADO"
    icone     = "✅" if aprovado else "❌"

    linhas = [
        "# Relatório de Qualidade de Dados — Sprint 2\n\n",
        f"**Gerado em:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  \n",
        f"**Limite de descarte configurado:** {LIMITE_DESCARTE_PCT * 100:.0f}%  \n",
        f"**Veredicto:** {icone} {veredicto}\n\n",
        "---\n\n",
        "## Resumo\n\n",
        "| Métrica | Valor |\n",
        "|---|---|\n",
        f"| Registros iniciais (Raw) | {relatorio['total_inicial']:,} |\n",
        f"| Registros finais (Trusted) | {relatorio['total_final']:,} |\n",
        f"| Total descartados | {relatorio['total_descartado']:,} |\n",
        f"| Percentual descartado | {relatorio['pct_descartado']:.1f}% |\n\n",
    ]

    if relatorio["descartes"]:
        linhas += [
            "## Detalhamento dos Descartes\n\n",
            "| Motivo | Registros | % do Total |\n",
            "|---|---|---|\n",
        ]
        for d in relatorio["descartes"]:
            linhas.append(
                f"| {d['motivo']} | {d['registros_descartados']:,} | {d['percentual']:.1f}% |\n"
            )
        linhas.append("\n")
    else:
        linhas.append("## Detalhamento dos Descartes\n\nNenhum descarte realizado.\n\n")

    linhas += [
        "---\n\n",
        "## Decisões de Limpeza\n\n",
        "> Descreva aqui as regras de negócio que motivaram cada decisão de descarte.\n",
        "> Este bloco é o único que requer edição manual.\n",
        ">\n",
        "> Exemplo:\n",
        "> - Registros com `valor < 0` descartados por inconsistência financeira.\n",
        "> - Duplicatas por `id_pedido` removidas mantendo o registro mais recente.\n",
        "> - Nulos em `categoria` descartados pois coluna é obrigatória no contrato.\n",
    ]

    caminho.write_text("".join(linhas), encoding="utf-8")
    logger.success(f"Relatório de qualidade salvo: {caminho}")
    return caminho


# ── Orquestração Principal ────────────────────────────────────────────────────
def transformar() -> Path:
    logger.info("Sprint 2: Saneamento & Qualidade (Trusted)")

    df            = _carregar_raw()
    df            = _validar_schema(df)
    df, relatorio = _limpar_e_tipar(df)
    _verificar_qualidade_minima(relatorio)
    _imprimir_relatorio(relatorio)
    caminho       = _exportar_trusted(df)
    _exportar_relatorio_md(relatorio)

    return caminho


if __name__ == "__main__":
    transformar()
    logger.info("Próximo passo: uv run python src/eda_qualidade.py")
