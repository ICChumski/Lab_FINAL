import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

INPUT_FILE = Path("data/processed/cycle_trips_prepared.csv")

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")


def get_engine():
    connection_string = (
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    return create_engine(connection_string)


def main():
    if not INPUT_FILE.exists():
        print(f"Arquivo não encontrado: {INPUT_FILE}")
        return

    engine = get_engine()

    print("Conectando ao PostgreSQL...")

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        conn.execute(text("DROP TABLE IF EXISTS raw.raw_cycle_trips;"))

    print("Lendo arquivo preparado...")
    df = pd.read_csv(INPUT_FILE)

    print(f"Total de linhas para carga: {len(df)}")

    print("Carregando dados na tabela raw.raw_cycle_trips...")
    df.to_sql(
        name="raw_cycle_trips",
        con=engine,
        schema="raw",
        if_exists="replace",
        index=False,
        chunksize=50000,
        method="multi",
    )

    print("Carga concluída com sucesso.")

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM raw.raw_cycle_trips;"))
        total = result.scalar()

    print(f"Total de linhas na tabela raw.raw_cycle_trips: {total}")


if __name__ == "__main__":
    main()