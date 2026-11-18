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

Na raiz do repositório, com Docker Desktop ativo, crie uma vez o arquivo local
`.env` com `ALAGA_AIRFLOW_DB_PASSWORD` definida para uma senha aleatória
alfanumérica. Mantenha esse arquivo fora do Git e guarde a mesma senha para
reutilizar o volume do PostgreSQL.

```bash
docker compose -f compose.airflow.yml up -d
```

Abra `http://127.0.0.1:8080` com o usuário `admin`. O `standalone` gera a senha
local em `/opt/airflow/standalone_admin_password.txt` no contêiner `airflow`.
O Compose monta `config/`, `dags/` e `chegada/` e
configura o Airflow 2.10.5 com `LocalExecutor` e PostgreSQL. Em **Admin →
Variables**, os diretórios podem ser ajustados por `alaga_chegada_dir` e
`alaga_config_dir`. Os valores padrão já correspondem aos caminhos do Compose.

Para reproduzir duas datas específicas, execute o backfill:

```bash
docker compose -f compose.airflow.yml exec -T airflow airflow dags backfill --start-date 2026-10-02T00:00:00-03:00 --end-date 2026-10-03T00:00:00-03:00 gs_coleta_alaga
```

Confira as execuções no Airflow. Clima, população e ocorrências podem rodar
juntas, e `conferir_chegada` começa quando as três terminam com sucesso.
Falhas da DAG são registradas no log do Airflow. Para manter a coleta diária,
ative a DAG pela interface. Com `catchup=True`, a ativação também agenda as
datas anteriores que ainda não foram processadas.

### 2. Databricks (lakehouse)
Se a equipe ainda não tiver workspace, crie um no
[Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition)
e confirme no catálogo o nome disponível antes de importar os notebooks.

1. No catálogo `workspace`, crie o esquema `gs` e o Volume gerenciado
   `chegada`. Se o catálogo tiver outro nome, ajuste as referências nos
   notebooks e nas consultas SQL. Envie os arquivos de `chegada/` **em 2 lotes**
   (ex.: lote 1 = dias 01–03/10; lote 2 = dias 04–06/10) — requisito 04.
2. Rode os notebooks em ordem:
   - `databricks/01_bronze.py` — Auto Loader incremental (rode 2× para provar que não duplica).
   - `databricks/02_prata.py` — tipos, MERGE, 3 regras de qualidade, mascaramento LGPD.
   - `databricks/03_ouro.py` — `dim_bairro`, `dim_tempo`, `fato_risco_bairro_hora`.
3. Execute `databricks/consultas_ouro.sql` (5 consultas, 1 de impacto).

### 3. Previsão vigente para o MVP

1. No Airflow, ative `gs_previsao_alaga` e dispare uma execução. A DAG
   coleta dados reais do Open-Meteo a cada hora, sem backfill, em
   `chegada/previsao/`. Para uma coleta avulsa, execute
   `python dags/alaga_previsao.py` na pasta `big-data`.
2. No Volume `workspace.gs.chegada`, crie a subpasta `previsao` e envie
   o JSON gerado. Execute `databricks/04_previsao_mvp.py` após os notebooks
   01 a 03. Esse notebook cria a Bronze e a Prata da previsão e a visão
   Ouro `workspace.gs.fato_previsao_bairro_hora`. A trilha diária continua
   armazenando as observações históricas.
3. Baixe o arquivo `part-*.csv` de `exportacao_mvp/` no Volume. No
   diretório do MVP, com a mesma `API_KEY` do servidor no ambiente, execute
   `python scripts/importar_ouro.py --url http://127.0.0.1:8000 --csv CAMINHO_DO_CSV --vigente`.
4. Abra **Meu bairro → Penha** e confira **Ouro (Databricks)** na página.
   Em `GET /risco/penha`, confirme `fonte: ouro` e compare a precipitação
   e o horário com a exportação. Horários no CSV têm fuso explícito UTC.
   O MVP tem previsão direta para as áreas que não receberam dados da Ouro.

A exportação filtra as próximas 12 horas e previsões coletadas nas últimas
3 horas. A transferência de arquivos e a publicação pelo importador são
manuais neste fluxo. Para demonstrar idempotência, reexecute o notebook
04 sem enviar arquivo novo e registre `novos=0` na Bronze.

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
├─ compose.airflow.yml            # Airflow LocalExecutor + PostgreSQL
├─ dags/                          # Coleta histórica e previsão horária
├─ config/bairros.json            # bairros de SP (lat/long + população)
├─ databricks/
│  ├─ 01_bronze.py  02_prata.py  03_ouro.py  04_previsao_mvp.py
│  └─ consultas_ouro.sql
├─ amostras/                      # um pouco de cada fonte (para o repositório)
├─ requirements.txt  .gitignore  README.md
```

> **Sem chaves, senhas ou dados pessoais reais no repositório.** As fontes usadas não
> exigem chave; ocorrências com dados pessoais são **simuladas** e mascaradas na Prata.

## Links

- Repositório (este): https://github.com/gukawamura/alaga-big-data
- Repositório GenAI (consome a camada Ouro): https://github.com/gukawamura/alaga-genai
