import logging
import requests
import pandas as pd

from io import StringIO
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# ===========================================================
# CONSTANTES
# ===========================================================

COLUNAS = [
    "DT_FECHAMENTO",
    "COD_MOEDA",
    "TIPO_MOEDA",
    "DESC_MOEDA",
    "TAXA_COMPRA",
    "TAXA_VENDA",
    "PARIDADE_COMPRA",
    "PARIDADE_VENDA",
]

POSTGRES_CONN_ID = "postgres_default"
SCHEMA = "public"
TABELA = "cotacoes"


def extrair(**kwargs) -> str:
    """
    Baixa o CSV de cotações do Banco Central para a data de execução
    e empurra o conteúdo bruto para o XCom.
    """
    ds_nodash = kwargs["ds_nodash"]
    base_url = Variable.get("BCB_BASE_URL")
    full_url = f"{base_url}/{ds_nodash}.csv"

    logging.warning(f"[EXTRAÇÃO] Baixando: {full_url}")

    response = requests.get(full_url, timeout=30)
    response.raise_for_status()

    logging.warning(
        f"[EXTRAÇÃO] Download concluído. Tamanho: {len(response.content)} bytes")

    # Empurra o conteúdo bruto para o XCom (próxima task vai ler)
    kwargs["ti"].xcom_push(key="csv_bruto", value=response.text)


def transformar(**kwargs) -> None:
    """
    Lê o CSV bruto do XCom, aplica os nomes de colunas,
    trata os tipos e empurra o DataFrame serializado para o XCom.
    """
    csv_bruto = kwargs["ti"].xcom_pull(key="csv_bruto", task_ids="extrair")

    logging.warning("[TRANSFORMAÇÃO] Iniciando transformação...")

    df = pd.read_csv(
        StringIO(csv_bruto),
        sep=";",
        header=None,
        names=COLUNAS,
        decimal=",",
        encoding="latin-1",
    )

    # Converte data
    df["DT_FECHAMENTO"] = pd.to_datetime(
        df["DT_FECHAMENTO"], format="%d/%m/%Y", errors="coerce")

    # Converte campos numéricos
    for col in ["TAXA_COMPRA", "TAXA_VENDA", "PARIDADE_COMPRA", "PARIDADE_VENDA"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Remove linhas completamente vazias
    df.dropna(how="all", inplace=True)

    logging.warning(f"[TRANSFORMAÇÃO] {len(df)} registros transformados.")
    logging.warning(f"[TRANSFORMAÇÃO] Preview:\n{df.head()}")

    # Empurra o DataFrame serializado para o XCom
    kwargs["ti"].xcom_push(key="df_transformado",
                           value=df.to_json(date_format="iso"))


def carregar(**kwargs) -> None:
    """
    Lê o DataFrame do XCom e insere os registros no PostgreSQL.
    Cria o schema e a tabela automaticamente se não existirem.
    """
    import json

    df_json = kwargs["ti"].xcom_pull(
        key="df_transformado", task_ids="transformar")
    df = pd.read_json(StringIO(df_json))

    logging.warning(f"[LOAD] Carregando {len(df)} registros no PostgreSQL...")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Cria schema e tabela se não existirem
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABELA} (
            id               SERIAL PRIMARY KEY,
            DT_FECHAMENTO    DATE,
            COD_MOEDA        VARCHAR(10),
            TIPO_MOEDA       VARCHAR(10),
            DESC_MOEDA       VARCHAR(100),
            TAXA_COMPRA      NUMERIC(18, 6),
            TAXA_VENDA       NUMERIC(18, 6),
            PARIDADE_COMPRA  NUMERIC(18, 6),
            PARIDADE_VENDA   NUMERIC(18, 6),
            criado_em        TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()

    # Insere os registros
    inseridos = 0
    for _, row in df.iterrows():
        cursor.execute(f"""
            INSERT INTO {SCHEMA}.{TABELA} (
                DT_FECHAMENTO, COD_MOEDA, TIPO_MOEDA, DESC_MOEDA,
                TAXA_COMPRA, TAXA_VENDA, PARIDADE_COMPRA, PARIDADE_VENDA
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row["DT_FECHAMENTO"],
            row["COD_MOEDA"],
            row["TIPO_MOEDA"],
            row["DESC_MOEDA"],
            row["TAXA_COMPRA"],
            row["TAXA_VENDA"],
            row["PARIDADE_COMPRA"],
            row["PARIDADE_VENDA"],
        ))
        inseridos += 1

    conn.commit()
    cursor.close()
    conn.close()

    logging.warning(f"[LOAD] {inseridos} registros inseridos com sucesso!")


with DAG(
    dag_id="cotacoes_bcb",
    description="ETL — Cotações de moedas do Banco Central do Brasil",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bcb", "etl", "cotacoes"],
) as dag:

    task_extrair = PythonOperator(
        task_id="extrair",
        python_callable=extrair,
        provide_context=True,
    )

    task_transformar = PythonOperator(
        task_id="transformar",
        python_callable=transformar,
        provide_context=True,
    )

    task_carregar = PythonOperator(
        task_id="carregar",
        python_callable=carregar,
        provide_context=True,
    )

    # Pipeline: Extração → Transformação → Load
    task_extrair >> task_transformar >> task_carregar
