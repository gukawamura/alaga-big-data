"""
Alaga Alerta · GS 2026.2 · Componente Dados (Big Data Architecture & Data Integration)

DAG de coleta: baixa dados reais do problema (risco de alagamento por bairro em SP)
e grava um arquivo por coleta na pasta `chegada/`, com a data no nome.

Requisitos atendidos aqui:
  02 · DAG agendado, salva em chegada/ um arquivo por coleta com a data no nome.
  03 · >=2 tarefas em paralelo + 1 tarefa que confere a chegada; config em Variables.

Fontes:
  - Open-Meteo (API pública, sem chave) ....... precipitação horária por bairro
  - IBGE (API pública, sem chave) ............. população do município de São Paulo
  - Ocorrências do cidadão (simuladas) ........ contêm dados pessoais -> mascarados na Prata/Ouro (LGPD)

Backfill: com catchup=True e start_date no passado, o Airflow gera arquivos de
vários dias (clima_2026-10-01.json, clima_2026-10-02.json, ...). Isso cumpre o
requisito 04 (>=2 lotes de dias diferentes) sem precisar esperar dias de calendário.
"""
from __future__ import annotations

import hashlib
import json
import random
from pathlib import Path

import pendulum
import requests

from airflow import DAG

# Operators mudaram de pacote no Airflow 3; importa com fallback para 2.x.
try:  # Airflow 3 (providers.standard)
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.python import PythonOperator
    from airflow.operators.empty import EmptyOperator

from airflow.models import Variable

# --------------------------------------------------------------------------- #
# Configuração (em Variables, nunca chaves no código).
# Defina no Airflow (Admin > Variables) ou use os defaults abaixo.
#   alaga_chegada_dir  -> pasta de pouso (ex.: /opt/airflow/chegada)
#   alaga_config_dir   -> pasta com bairros.json (ex.: /opt/airflow/config)
# --------------------------------------------------------------------------- #
CHEGADA = Path(Variable.get("alaga_chegada_dir", default_var="/opt/airflow/chegada"))
CONFIG = Path(Variable.get("alaga_config_dir", default_var="/opt/airflow/config"))

IBGE_MUNICIPIO_SP = "3550308"
NOMES = ["Ana", "Bruno", "Carla", "Diego", "Elena", "Felipe", "Gabi", "Heitor", "Iris", "João"]
SOBRENOMES = ["Silva", "Souza", "Oliveira", "Santos", "Lima", "Costa", "Pereira", "Almeida"]


def _carregar_bairros() -> list[dict]:
    return json.loads((CONFIG / "bairros.json").read_text(encoding="utf-8"))


def _gravar(subpasta: str, nome_arquivo: str, conteudo: dict) -> str:
    destino = CHEGADA / subpasta / nome_arquivo
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(json.dumps(conteudo, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(destino)


# --------------------------------------------------------------------------- #
# Tarefa 1 (paralela): clima por bairro via Open-Meteo
# --------------------------------------------------------------------------- #
def coletar_clima(ds, **_):
    bairros = _carregar_bairros()
    lats = ",".join(str(b["latitude"]) for b in bairros)
    lons = ",".join(str(b["longitude"]) for b in bairros)
    r = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude": lats,
            "longitude": lons,
            "start_date": ds,
            "end_date": ds,
            "hourly": "precipitation,temperature_2m",
            "timezone": "America/Sao_Paulo",
        },
        timeout=60,
    )
    r.raise_for_status()
    dados = r.json()
    # Open-Meteo devolve uma lista (um objeto por coordenada) quando há várias.
    blocos = dados if isinstance(dados, list) else [dados]
    registros = []
    for b, bloco in zip(bairros, blocos):
        registros.append({
            "bairro_id": b["bairro_id"],
            "bairro": b["bairro"],
            "data_coleta": ds,
            "hourly": bloco.get("hourly", {}),
        })
    caminho = _gravar("clima", f"clima_{ds}.json", {"data_coleta": ds, "registros": registros})
    print(f"[clima] {len(registros)} bairros -> {caminho}")


# --------------------------------------------------------------------------- #
# Tarefa 2 (paralela): população do município de SP via IBGE
# --------------------------------------------------------------------------- #
def coletar_populacao(ds, **_):
    # Agregado 6579 = população residente estimada; variável 9324.
    url = (
        "https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/-1/"
        f"variaveis/9324?localidades=N6[{IBGE_MUNICIPIO_SP}]"
    )
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    payload = r.json()
    caminho = _gravar("populacao", f"populacao_{ds}.json",
                      {"data_coleta": ds, "fonte": "IBGE/6579", "payload": payload})
    print(f"[populacao] IBGE SP -> {caminho}")


# --------------------------------------------------------------------------- #
# Tarefa 3 (paralela): ocorrências do cidadão (SIMULADAS, com dados pessoais)
# Geração determinística por ds (reprodutível no backfill). Serão mascaradas na Prata.
# --------------------------------------------------------------------------- #
def coletar_ocorrencias(ds, **_):
    rng = random.Random(ds)  # seed = data -> mesmo ds gera sempre os mesmos dados
    bairros = _carregar_bairros()
    ocorrencias = []
    for _ in range(rng.randint(4, 12)):
        b = rng.choice(bairros)
        nome = f"{rng.choice(NOMES)} {rng.choice(SOBRENOMES)}"
        telefone = f"11 9{rng.randint(1000, 9999)}-{rng.randint(1000, 9999)}"
        cpf = f"{rng.randint(100,999)}.{rng.randint(100,999)}.{rng.randint(100,999)}-{rng.randint(10,99)}"
        ocorrencias.append({
            "protocolo": hashlib.md5(f"{ds}{nome}{telefone}".encode()).hexdigest()[:10],
            "bairro_id": b["bairro_id"],
            "data_hora": f"{ds}T{rng.randint(0,23):02d}:{rng.randint(0,59):02d}:00",
            "nome": nome,          # DADO PESSOAL -> mascarar
            "telefone": telefone,  # DADO PESSOAL -> mascarar
            "cpf": cpf,            # DADO PESSOAL -> mascarar
            "relato": rng.choice([
                "Rua começando a alagar", "Bueiro entupido", "Água subindo rápido",
                "Carro parado na água", "Córrego transbordando",
            ]),
            "nivel_agua_cm": rng.choice([10, 20, 35, 50, 80, 120]),
        })
    caminho = _gravar("ocorrencias", f"ocorrencias_{ds}.json",
                      {"data_coleta": ds, "ocorrencias": ocorrencias})
    print(f"[ocorrencias] {len(ocorrencias)} relatos -> {caminho}")


# --------------------------------------------------------------------------- #
# Tarefa 4: confere se os três arquivos da coleta chegaram
# --------------------------------------------------------------------------- #
def conferir_chegada(ds, **_):
    esperados = [
        CHEGADA / "clima" / f"clima_{ds}.json",
        CHEGADA / "populacao" / f"populacao_{ds}.json",
        CHEGADA / "ocorrencias" / f"ocorrencias_{ds}.json",
    ]
    faltando = [str(p) for p in esperados if not p.exists()]
    if faltando:
        raise FileNotFoundError(f"Arquivos não encontrados para {ds}: {faltando}")
    print(f"[conferir] OK — 3 arquivos presentes para {ds}")


with DAG(
    dag_id="gs_coleta_alaga",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/Sao_Paulo"),
    catchup=True,
    max_active_runs=1,
    tags=["gs", "alaga-alerta", "big-data"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")

    clima = PythonOperator(task_id="coletar_clima", python_callable=coletar_clima)
    populacao = PythonOperator(task_id="coletar_populacao", python_callable=coletar_populacao)
    ocorrencias = PythonOperator(task_id="coletar_ocorrencias", python_callable=coletar_ocorrencias)

    conferir = PythonOperator(task_id="conferir_chegada", python_callable=conferir_chegada)

    inicio >> [clima, populacao, ocorrencias] >> conferir
