# Alaga Alerta — Componente **Dados** (Big Data Architecture & Data Integration)

Pipeline que coleta dados reais de risco de alagamento por bairro em São Paulo,
trata em camadas **Bronze → Prata → Ouro** e entrega ao MVP o risco por bairro e hora.

> **Produto em uma frase:** o Alaga Alerta avisa moradores e motoristas, por bairro e em
> tempo útil, quando uma rua está prestes a alagar e o que fazer.
>
> **Pergunta que o MVP responde:** *qual o risco de alagamento no meu bairro nas próximas
> horas e quantas pessoas estão expostas?*

## Integrantes

- Felipe Capriotti — RM98460
- Manoella Herrerias Waideman — RM98906
- Victor Hugo Andrade — RM550996
- Gustavo Kawamura — RM99679

## Arquitetura

```
Fontes (APIs)                Airflow (coleta)         Databricks Free Edition
┌─────────────┐   DAG @daily  ┌──────────────┐  upload  ┌───────────────────────────┐
│ Open-Meteo  │──┐ catchup   │ chegada/      │  manual  │ Volume → Bronze → Prata → │
│ IBGE        │──┼──────────▶│  clima/       │────────▶ │   Ouro (fato + 2 dims)    │
│ Ocorrências │──┘           │  populacao/   │          └────────────┬──────────────┘
│ (simuladas) │              │  ocorrencias/ │                       │ SQL
└─────────────┘              └──────────────┘                        ▼
                                                            API central → MVP (tela do cidadão)
```

O Databricks Free Edition **não se conecta ao Airflow**: os arquivos de `chegada/` sobem
ao Volume pela interface, em **lotes** (daí o requisito 04 de 2 lotes de dias diferentes).

## Fontes de dados

| Fonte | Tipo | Formato | Frequência | Conteúdo |
|---|---|---|---|---|
| Open-Meteo (`archive-api`) | API pública real (sem chave) | JSON | diária (backfill) | precipitação e temperatura horária por bairro |
| IBGE (`agregados/6579`) | API pública real (sem chave) | JSON | diária | população do município de SP (contexto/impacto) |
| Ocorrências do cidadão | Simulada, documentada | JSON | diária | relatos com dados pessoais → **mascarados** (LGPD) |

População por bairro em `config/bairros.json` (derivada de dados abertos; usada em `dim_bairro`).

## Como reproduzir

### 1. Airflow (coleta)
```bash
# com Astro CLI (recomendado no Windows + Docker Desktop)
astro dev init          # só na 1ª vez, na pasta big-data/
astro dev start
```
No Airflow UI (**Admin → Variables**) crie:
- `alaga_chegada_dir` = `/opt/airflow/chegada`
- `alaga_config_dir` = `/opt/airflow/config`

Copie `config/bairros.json` para a pasta montada e ligue a DAG **`gs_coleta_alaga`**.
Com `catchup=True`, o backfill gera vários dias de arquivos em `chegada/`.

### 2. Databricks (lakehouse)
1. Crie o Volume `workspace.gs.chegada` e envie os arquivos de `chegada/` **em 2 lotes**
   (ex.: lote 1 = dias 01–03/10; lote 2 = dias 04–06/10) — requisito 04.
2. Rode os notebooks em ordem:
   - `databricks/01_bronze.py` — Auto Loader incremental (rode 2× para provar que não duplica).
   - `databricks/02_prata.py` — tipos, MERGE, 3 regras de qualidade, mascaramento LGPD.
   - `databricks/03_ouro.py` — `dim_bairro`, `dim_tempo`, `fato_risco_bairro_hora`.
3. Execute `databricks/consultas_ouro.sql` (5 consultas, 1 de impacto).

## Mapa requisitos → onde está

| # | Requisito | Onde |
|---|---|---|
| 01 | ≥2 fontes, ≥1 API pública | `dags/gs_coleta_alaga.py` (Open-Meteo + IBGE) |
| 02 | DAG agendado, 1 arquivo/coleta com data no nome | `coletar_*` → `chegada/<fonte>/<fonte>_{ds}.json` |
| 03 | ≥2 tarefas paralelas + 1 confere; chaves em Variables | `inicio >> [clima, populacao, ocorrencias] >> conferir` |
| 04 | ≥2 lotes de dias diferentes no Volume | upload manual em 2 lotes (backfill) |
| 05 | Bronze incremental sem duplicar + contagens | `01_bronze.py` (Auto Loader + checkpoint) |
| 06 | Prata: tipos, MERGE, 3 regras com total barrado | `02_prata.py` (`aplicar_regras`, `merge_prata`) |
| 07 | Ouro: 1 fato + 2 dims + 5 SQL (1 impacto) | `03_ouro.py` + `consultas_ouro.sql` |
| 08 | LGPD na Ouro + tela do MVP | hash SHA-256 em `02_prata.py`; fato sem dado pessoal |
| 09 | README + organização | este arquivo |

## Estrutura do repositório
```
big-data/
├─ dags/gs_coleta_alaga.py        # DAG de coleta (Airflow)
├─ config/bairros.json            # bairros de SP (lat/long + população)
├─ databricks/
│  ├─ 01_bronze.py  02_prata.py  03_ouro.py
│  └─ consultas_ouro.sql
├─ amostras/                      # um pouco de cada fonte (para o repositório)
├─ requirements.txt  .gitignore  README.md
```

> **Sem chaves, senhas ou dados pessoais reais no repositório.** As fontes usadas não
> exigem chave; ocorrências com dados pessoais são **simuladas** e mascaradas na Prata.

## Links

- Repositório (este): https://github.com/gukawamura/alaga-big-data
- Repositório GenAI (consome a camada Ouro): https://github.com/gukawamura/alaga-genai
