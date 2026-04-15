import os
from pathlib import Path
from datetime import datetime
import xml.etree.ElementTree as ET

import requests
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta

load_dotenv()

BRONZE_BASE_URL = os.getenv("BRONZE_BASE_URL")
BRONZE_LISTING_URL = os.getenv("BRONZE_LISTING_URL")
BRONZE_MONTHS_BACK = int(os.getenv("BRONZE_MONTHS_BACK", 3))

DOWNLOAD_DIR = Path("data/landing")
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


def get_target_months(months_back: int) -> list[str]:
    current = datetime.today().replace(day=1)
    months = []

    for i in range(months_back):
        dt = current - relativedelta(months=i)
        months.append(dt.strftime("%Y-%m"))

    return months


def list_bucket_files(listing_url: str) -> list[str]:
    response = requests.get(listing_url, timeout=60)
    response.raise_for_status()

    root = ET.fromstring(response.text)

    keys = []
    for elem in root.iter():
        if elem.tag.endswith("Key") and elem.text:
            keys.append(elem.text)

    return keys


import re
from datetime import datetime

def extract_end_date(filename: str):
    """
    Extrai a data final do nome do arquivo.
    Ex:
    01aJourneyDataExtract10Jan16-23Jan16.csv
    103JourneyDataExtract28Mar2018-03Apr2018.csv
    112JourneyDataExtract30May2018-05June2018.csv
    """
    pattern = r'(\d{2})([A-Za-z]+)(\d{2,4})-(\d{2})([A-Za-z]+)(\d{2,4})'
    match = re.search(pattern, filename)

    if not match:
        return None

    _, _, _, day2, month2, year2 = match.groups()

    month_map = {
        "Jan": "Jan", "Feb": "Feb", "Mar": "Mar", "Apr": "Apr",
        "May": "May", "Jun": "Jun", "June": "Jun",
        "Jul": "Jul", "Aug": "Aug", "Sep": "Sep", "Oct": "Oct",
        "Nov": "Nov", "Dec": "Dec"
    }

    month2 = month_map.get(month2, month2)

    if len(year2) == 2:
        year2 = "20" + year2

    try:
        return datetime.strptime(f"{day2}{month2}{year2}", "%d%b%Y")
    except ValueError:
        return None


def filter_recent_files(keys: list[str], limit: int = 20) -> list[str]:
    csv_files = [k for k in keys if k.lower().endswith(".csv")]

    dated_files = []
    for key in csv_files:
        end_date = extract_end_date(key)
        if end_date is not None:
            dated_files.append((key, end_date))

    dated_files.sort(key=lambda x: x[1], reverse=True)

    return [key for key, _ in dated_files[:limit]]


def download_file(base_url: str, key: str) -> None:
    filename = Path(key).name
    output_path = DOWNLOAD_DIR / filename

    relative_path = key.replace("usage-stats/", "", 1)
    file_url = f"{base_url}/{relative_path}"

    print(f"Baixando: {file_url}")

    response = requests.get(file_url, timeout=120)
    response.raise_for_status()

    with open(output_path, "wb") as f:
        f.write(response.content)

    print(f"Salvo em: {output_path}")


def main():
    if not BRONZE_BASE_URL or not BRONZE_LISTING_URL:
        raise ValueError("BRONZE_BASE_URL e BRONZE_LISTING_URL precisam estar definidos no .env")

    target_months = get_target_months(BRONZE_MONTHS_BACK)
    print("Meses alvo:", target_months)

    keys = list_bucket_files(BRONZE_LISTING_URL)
    print(f"Total de arquivos listados: {len(keys)}")

    print("\nExemplos de arquivos encontrados:")
    for key in keys[:30]:
        print(key)

    print("\nÚltimos 30 arquivos encontrados:")
    for key in keys[-30:]:
        print(key)

    # 🔎 CHECAGEM DE ARQUIVOS RECENTES
    print("\n🔎 Procurando arquivos com 2025, 2026, zip ou padrões diferentes...\n")

    keywords = ["2025", "2026", "Dec2025", "Jan2025", ".zip"]
    found = []

    for key in keys:
        if any(k.lower() in key.lower() for k in keywords):
            found.append(key)

    if found:
        print("Arquivos encontrados com esses padrões:\n")
        for f in found[:50]:
            print(f)
    else:
        print("❌ Nenhum arquivo com 2025/2026 ou zip encontrado.")

    # DOWNLOAD DOS ÚLTIMOS ARQUIVOS
    files_to_download = filter_recent_files(keys, limit=20)

    print("\nArquivos selecionados para download:")
    for file_key in files_to_download:
        print("-", file_key)

    print("\nIniciando downloads...\n")

    for file_key in files_to_download:
        try:
            download_file(BRONZE_BASE_URL, file_key)
        except Exception as e:
            print(f"Erro ao baixar {file_key}: {e}")

    print("\nConcluído.")


if __name__ == "__main__":
    main()