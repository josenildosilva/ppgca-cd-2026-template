# 🎓 PPGCA 2026 — Projeto de Ciência de Dados

> **Mestrado Profissional em Ciência de Dados**
> Prof. Josenildo Silva · IFMA · Turma 2026.1

---

## 👥 Grupo

| Nome | Matrícula | E-mail |
|------|-----------|--------|
| Integrante 1 | ... | ... |
| Integrante 2 | ... | ... |
| Integrante 3 | ... | ... |

---

## 📋 Sobre o Projeto

> **TODO:** Descreva o problema de negócio escolhido pelo grupo.
>
> **Pergunta Central:** Qual é a pergunta de negócio que este projeto responde?
>
> **Fonte de Dados:** Qual API ou dataset foi utilizado?

---

## 🚀 Como Rodar (Zero Config)

> **Regra de Ouro:** O professor clona este repositório, executa os comandos abaixo
> e tudo funciona. "Funciona na minha máquina" é **falha grave**.

### Pré-requisitos

- [uv](https://docs.astral.sh/uv/getting-started/installation/) instalado

### Setup Inicial

```bash
# 1. Clone o repositório
git clone https://github.com/seu-usuario/ppgca-cd-2026.git
cd ppgca-cd-2026

# 2. Configure as variáveis de ambiente
cp .env.example .env
# Edite o .env com suas credenciais (ex: API_KEY)

# 3. Instale as dependências
uv sync
```

### Executar o Pipeline Completo

```bash
# Sprint 1 — Ingestão (coleta da API para data/raw)
uv run python src/ingest.py

# Sprint 2 — Qualidade (limpeza + validação Pandera → data/trusted)
uv run python src/transform.py

# Sprint 3 — Modelagem (Star Schema + Feature Store → data/mart, data/feat)
uv run python src/model.py

# Sprint 4 — Data App (dashboard interativo)
uv run streamlit run src/app.py

# Sprint 4 — Relatório (HTML reprodutível)
uv run quarto render reports/report.qmd
```

---

## 🏗️ Arquitetura do Projeto

```
ppgca-cd-2026/
│
├── src/                        # Código-fonte principal
│   ├── ingest.py               # S1: Coleta da API → data/raw
│   ├── transform.py            # S2: Limpeza + Pandera → data/trusted
│   ├── model.py                # S3: Star Schema + Features → data/mart, feat
│   └── app.py                  # S4: Dashboard Streamlit
│
├── data/                       # Camadas do Data Lake (não versionadas)
│   ├── raw/                    # Dados brutos e imutáveis (JSON)
│   ├── trusted/                # Dados validados (Parquet)
│   ├── mart/                   # Tabelas analíticas (Fato/Dimensão)
│   └── feat/                   # Feature Store para ML
│
├── notebooks/                  # Análises exploratórias (EDA)
├── reports/
│   ├── report.qmd              # S4: Relatório Quarto (fonte)
│   └── data_dictionary.md      # S3: Dicionário de dados (gerado)
│
├── .github/
│   └── workflows/ci.yml        # CI: Verifica reprodutibilidade
│
├── .env.example                # Template de variáveis de ambiente
├── .gitignore                  # Ignora dados, .env, .venv
├── pyproject.toml              # Dependências do projeto (uv)
└── README.md                   # Este arquivo
```

### Fluxo de Dados

```
API/Fonte
    ↓  ingest.py
[data/raw]  ← Imutável. Jamais modificado manualmente.
    ↓  transform.py (DuckDB + Pandera)
[data/trusted]  ← Dado limpo e validado (Parquet).
    ↓  model.py (DuckDB SQL)
[data/mart]     ← Star Schema (BI): fato_principal + dimensões
[data/feat]     ← Feature Store (ML): features.parquet
    ↓  app.py / report.qmd
Dashboard Streamlit + Relatório HTML
```

---

## 🛠️ Stack Tecnológica

| Ferramenta | Papel |
|------------|-------|
| `uv` | Gerenciamento de pacotes e ambiente virtual |
| `DuckDB` | Engine OLAP — processamento analítico local |
| `Pandera` | Contratos de dados e validação de schema |
| `Streamlit` | Data app interativo |
| `Quarto` | Relatório técnico reprodutível |
| `Plotly` | Visualizações interativas |

---

## 📦 Sprints e Entregas

| Sprint | Prazo | Peso | Entrega |
|--------|-------|------|---------|
| **S1 — Infraestrutura** | Semana 02 | 10% | `ingest.py` + repositório configurado |
| **S2 — Qualidade** | Semana 06 | 20% | `transform.py` + `data/trusted/*.parquet` |
| **S3 — Modelagem** | Semana 10 | 25% | `model.py` + Star Schema + Feature Store |
| **S4 — Produto** | Semana 13 | 25% | `app.py` + `reports/report.html` |
| **S5 — Demo Day** | Semana 15 | 20% | Pitch + release v1.0 |

---

## 🔒 Segurança

- **Nunca comite o `.env`** — ele está no `.gitignore`.
- Use `.env.example` como referência para configuração.
- Credenciais (`API_KEY`, senhas) ficam **exclusivamente** no `.env`.

---

## 📚 Referências

- REIS, Joe; HOUSLEY, Matt. *Fundamentals of Data Engineering*. O'Reilly, 2022.
- IGUAL, L.; SEGUÍ, S. *Introduction to Data Science*. Springer, 2017.
- FAWCETT, T.; PROVOST, F. *Data Science para Negócios*. Alta Books, 2016.
- [Documentação DuckDB](https://duckdb.org/docs/)
- [Documentação Streamlit](https://docs.streamlit.io/)
- [Documentação Pandera](https://pandera.readthedocs.io/)
