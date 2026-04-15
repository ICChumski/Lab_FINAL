from pathlib import Path
import pandas as pd

INPUT_FILE = Path("data/processed/cycle_trips.csv")
OUTPUT_FILE = Path("data/processed/cycle_trips_prepared.csv")
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)


def main():
    if not INPUT_FILE.exists():
        print(f"Arquivo não encontrado: {INPUT_FILE}")
        return

    print(f"Lendo arquivo: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)

    print(f"Total de linhas lidas: {len(df)}")
    print(f"Total de colunas lidas: {len(df.columns)}")

    rename_map = {
        "Number": "trip_id",
        "Start date": "start_date",
        "Start station": "start_station_name",
        "Start station number": "start_station_id",
        "End date": "end_date",
        "End station": "end_station_name",
        "End station number": "end_station_id",
        "Bike number": "bike_id",
        "Bike model": "bike_model",
        "Total duration": "total_duration",
        "Total duration (ms)": "total_duration_ms",
        "source_file": "source_file",
    }

    df = df.rename(columns=rename_map)

    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")

    print("\nColunas após padronização:")
    for i, col in enumerate(df.columns.tolist(), start=1):
        print(f"{i}. {col}")

    print("\nTipos das colunas:")
    print(df.dtypes)

    print("\nValores nulos por coluna:")
    print(df.isnull().sum())

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nArquivo preparado salvo em: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()