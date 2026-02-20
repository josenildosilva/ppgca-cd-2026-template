"""
Sprint 4 — Data App (app.py)
==============================
Dashboard Streamlit conectado à camada mart (DuckDB/Parquet).
Narrativa: Contexto → Dados → Conclusão.

COMO RODAR:
    uv run streamlit run src/app.py
"""

from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

# ── Configuração da Página ────────────────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard — PPGCA 2026",
    page_icon="📊",
    layout="wide",
)

# ── Conexão com os Dados (Caching para Performance) ───────────────────────────
MART_DIR = Path("data/mart")
DB_PATH = Path("data/analytics.duckdb")


@st.cache_data(ttl=3600)  # Cache por 1 hora
def load_data() -> pd.DataFrame:
    """Carrega os dados da camada mart via DuckDB."""
    con = duckdb.connect(str(DB_PATH), read_only=True)
    df = con.execute("""
        SELECT
            f.*,
            t.ano,
            t.mes,
            t.trimestre
        FROM fato_principal f
        LEFT JOIN dim_tempo t ON f.data = t.data
        ORDER BY f.data
    """).df()
    con.close()
    return df


# ── Layout Principal ──────────────────────────────────────────────────────────
st.title("📊 Dashboard Analítico")
st.caption("PPGCA 2026 · Mestrado Profissional em Ciência de Dados")

# ── 1. CONTEXTO (por que esse dado importa?) ──────────────────────────────────
st.markdown("""
> **Contexto:** Descreva aqui o problema de negócio que este dashboard resolve.
> Qual a fonte dos dados? Qual o período analisado? Qual a pergunta central?
""")

st.divider()

# ── 2. DADOS & FILTROS ────────────────────────────────────────────────────────
with st.spinner("Carregando dados..."):
    df = load_data()

# Sidebar com filtros
with st.sidebar:
    st.header("🔧 Filtros")

    if "ano" in df.columns:
        anos = sorted(df["ano"].dropna().unique().tolist())
        ano_sel = st.multiselect("Ano", anos, default=anos)
        df = df[df["ano"].isin(ano_sel)]

    # TODO: Adicione mais filtros relevantes para o seu dataset

st.subheader("📈 Visão Geral")

# KPIs em colunas
col1, col2, col3 = st.columns(3)
with col1:
    total = df["valor"].sum() if "valor" in df.columns else 0
    st.metric("Total Acumulado", f"{total:,.2f}")
with col2:
    media = df["valor"].mean() if "valor" in df.columns else 0
    st.metric("Média", f"{media:,.2f}")
with col3:
    registros = len(df)
    st.metric("Registros", f"{registros:,}")

st.divider()

# ── 3. GRÁFICOS ───────────────────────────────────────────────────────────────
st.subheader("📉 Evolução Temporal")

if "data" in df.columns and "valor" in df.columns:
    fig = px.line(
        df.groupby("data")["valor"].sum().reset_index(),
        x="data",
        y="valor",
        title="Valor ao longo do tempo",
        labels={"data": "Data", "valor": "Valor"},
    )
    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Configure as colunas corretas do seu dataset nos gráficos acima.")

# TODO: Adicione mais visualizações relevantes para o seu dataset

st.divider()

# ── 4. CONCLUSÃO ──────────────────────────────────────────────────────────────
st.subheader("💡 Conclusões & Recomendações")
st.markdown("""
> **TODO:** Descreva aqui as principais descobertas da análise e
> as recomendações de negócio para o tomador de decisão.
>
> Estrutura sugerida:
> 1. **Principal descoberta** — O que os dados revelam?
> 2. **Oportunidade** — O que pode ser melhorado?
> 3. **Ação recomendada** — O que fazer agora?
""")

# ── Rodapé ────────────────────────────────────────────────────────────────────
st.divider()
st.caption("Desenvolvido por [Nome do Grupo] · PPGCA 2026 · Prof. Josenildo Silva")
