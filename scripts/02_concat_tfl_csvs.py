from pathlib import Path
import pandas as pd

INPUT_DIR = Path("data/landing")
OUTPUT_FILE = Path("data/processed/cycle_trips.csv")
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)


def main():
    csv_files = sorted(INPUT_DIR.glob("*.csv"))

    # evita reprocessar um arquivo consolidado antigo
    csv_files = [f for f in csv_files if f.name != "cycle_trips.csv"]

    if not csv_files:
        print("Nenhum CSV encontrado em data/landing")
        return

    print("Arquivos encontrados para concatenação:")
    for f in csv_files:
        print("-", f.name)

    dataframes = []

    for file in csv_files:
        try:
            df = pd.read_csv(file)
            df["source_file"] = file.name
            dataframes.append(df)
            print(f"Lido com sucesso: {file.name} | {len(df)} linhas")
        except Exception as e:
            print(f"Erro ao ler {file.name}: {e}")

    if not dataframes:
        print("Nenhum arquivo foi lido com sucesso.")
        return

    df_final = pd.concat(dataframes, ignore_index=True)

    print("\nResumo final:")
    print(f"Total de linhas: {len(df_final)}")
    print(f"Total de colunas: {len(df_final.columns)}")

    print("\nColunas encontradas:")
    for i, col in enumerate(df_final.columns.tolist(), start=1):
        print(f"{i}. {col}")

    print("\nPrimeiras 5 linhas:")
    print(df_final.head())

    df_final.to_csv(OUTPUT_FILE, index=False)
    print(f"\nArquivo consolidado salvo em: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()