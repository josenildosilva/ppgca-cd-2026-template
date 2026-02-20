"""
Sprint 1 — Ingestão de Dados (ingest.py)
=========================================
Responsável por coletar dados da fonte (API/CSV/etc.) e
salvar na camada RAW (imutável, dado bruto).

REGRAS:
  - Este script deve ser IDEMPOTENTE (rodar várias vezes sem duplicar dados).
  - Credenciais vêm do .env, nunca hardcoded aqui.
  - Saída: data/raw/<nome_arquivo>.<extensão>
"""

import json
import os
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

# ── Configuração ──────────────────────────────────────────────────────────────
load_dotenv()

API_KEY = os.getenv("API_KEY")
API_BASE_URL = os.getenv("API_BASE_URL", "https://api.exemplo.gov.br/v1")
RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ── Funções ───────────────────────────────────────────────────────────────────

def fetch_data(endpoint: str, params: dict | None = None) -> list[dict]:
    """
    Realiza uma requisição GET ao endpoint da API.
    Retorna uma lista de registros (JSON).
    """
    headers = {"Authorization": f"Bearer {API_KEY}"} if API_KEY else {}
    url = f"{API_BASE_URL}/{endpoint}"

    print(f"[INGEST] Buscando dados em: {url}")
    response = requests.get(url, headers=headers, params=params or {}, timeout=30)
    response.raise_for_status()

    data = response.json()
    print(f"[INGEST] {len(data)} registros recebidos.")
    return data


def save_raw(data: list[dict], name: str) -> Path:
    """
    Salva os dados brutos em JSON na camada raw com timestamp.
    Estratégia: Full Load com data de coleta no nome do arquivo.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = RAW_DIR / f"{name}_{timestamp}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"[INGEST] Dados salvos em: {output_path}")
    return output_path


# ── Ponto de Entrada ──────────────────────────────────────────────────────────

def main():
    # TODO: Substitua "endpoint-exemplo" pelo endpoint real da sua API
    data = fetch_data("endpoint-exemplo", params={"limit": 1000})
    save_raw(data, name="dataset")
    print("[INGEST] ✅ Sprint 1 concluído.")


if __name__ == "__main__":
    main()
