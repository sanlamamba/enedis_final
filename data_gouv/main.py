import re
import requests
from urllib.parse import urljoin
import os
import io
import zipfile
import tempfile
from google.cloud import storage


def get_month_from_trimestre(tri: int) -> int:
    return {1: 3, 2: 6, 3: 9, 4: 12}.get(tri, -1)


def get_standard_trimestre(month: int) -> int:
    if 1 <= month <= 3:
        return 1
    elif 4 <= month <= 6:
        return 2
    elif 7 <= month <= 9:
        return 3
    elif 10 <= month <= 12:
        return 4
    return -1


def adjust_trimestre(std_tri: int) -> int:
    return std_tri - 1 if std_tri > 1 else 4


def fetch_html_content(url: str) -> str:
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        exit(1)


def extract_filtered_links(html: str, base_resource: str) -> list:
    raw_links = re.findall(
        r'"([^"\']+\.(?:zip|7z))"|\'([^\\"\']+\.(?:zip|7z))\'', html, re.IGNORECASE
    )
    links = [l[0] if l[0] else l[1] for l in raw_links]
    filtered = [
        urljoin(base_resource, link) if link.startswith("/") else link
        for link in links
        if re.search(r"immeub|immeule", link, re.IGNORECASE)
    ]
    return filtered


def parse_filename(filename: str) -> dict:
    m_date = re.search(r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})", filename)
    if m_date:
        year = int(m_date.group("year"))
        month = int(m_date.group("month"))
        tri = adjust_trimestre(get_standard_trimestre(month))
        return {"year": year, "trimestre": tri, "month": month}

    m_simple = re.search(r"(?P<year>\d{4})t+?(?P<tri>\d)", filename, re.IGNORECASE)
    if m_simple:
        year = int(m_simple.group("year"))
        tri = int(m_simple.group("tri"))
        return {"year": year, "trimestre": tri, "month": get_month_from_trimestre(tri)}

    m_dash = re.search(r"(?P<year>\d{4})-t(?P<tri>\d)", filename, re.IGNORECASE)
    if m_dash:
        year = int(m_dash.group("year"))
        tri = int(m_dash.group("tri"))
        return {"year": year, "trimestre": tri, "month": get_month_from_trimestre(tri)}

    return {}


def deduplicate_results(results: list) -> list:
    return [dict(t) for t in {tuple(d.items()) for d in results}]


def sort_results(results: list) -> list:
    return sorted(results, key=lambda x: (x["year"], x["trimestre"]))


def extract_and_upload(
    buffer: io.BytesIO, file_year: int, file_tri: int, file_extension: str, bucket
):
    buffer.seek(0)
    if file_extension == "zip":
        try:
            with zipfile.ZipFile(buffer, "r") as zip_ref:
                for member in zip_ref.infolist():
                    if member.is_dir():
                        continue
                    extracted_data = zip_ref.read(member)
                    extracted_blob_name = f"processed/{member.filename}"
                    blob = bucket.blob(extracted_blob_name)
                    blob.upload_from_string(extracted_data)
                    print(f"Extracted and uploaded {extracted_blob_name}")
        except Exception as e:
            print(f"Failed to extract ZIP archive: {e}")
    elif file_extension == "7z":
        try:
            import py7zr

            with tempfile.TemporaryDirectory() as tmpdirname:
                with py7zr.SevenZipFile(buffer, mode="r") as archive:
                    archive.extractall(path=tmpdirname)
                for root, _, files in os.walk(tmpdirname):
                    for f in files:
                        local_path = os.path.join(root, f)
                        rel_path = os.path.relpath(local_path, tmpdirname)
                        extracted_blob_name = (
                            f"processed/{file_year}_T{file_tri}/{rel_path}"
                        )
                        blob = bucket.blob(extracted_blob_name)
                        blob.upload_from_filename(local_path)
                        print(f"Extracted and uploaded {extracted_blob_name}")
        except ImportError:
            print("py7zr is not installed, skipping 7z file extraction.")
        except Exception as e:
            print(f"Failed to extract 7z archive: {e}")
    else:
        print(f"No extraction logic for file type: {file_extension}")


def read_history_from_gcp(bucket, history_blob_name="downloaded/history.csv"):
    history_blob = bucket.blob(history_blob_name)
    history = []
    if history_blob.exists():
        content = history_blob.download_as_text()
        lines = content.strip().split("\n")
        for line in lines:
            parts = line.strip().split(",")
            if len(parts) == 3:
                history.append(
                    {
                        "year": int(parts[0]),
                        "trimestre": int(parts[1]),
                        "link": parts[2],
                    }
                )
    return history


def write_history_to_gcp(bucket, history, history_blob_name="downloaded/history.csv"):
    content = "\n".join(
        f"{rec['year']},{rec['trimestre']},{rec['link']}" for rec in history
    )
    blob = bucket.blob(history_blob_name)
    blob.upload_from_string(content)
    print("Updated history.csv in GCP.")


def upload_files_to_gcp(results: list, bucket):
    history = read_history_from_gcp(bucket)
    new_entries = []
    for rec in results:
        if rec in history:
            print(f"Already processed: {rec['link']}")
            continue

        file_extension = rec["link"].split(".")[-1].lower()
        if file_extension not in ["zip", "7z"]:
            print(f"Skipping non-compressed file: {rec['link']}")
            continue

        file_year = rec["year"]
        file_tri = rec["trimestre"]
        filename = f"downloaded/{file_year}_T{file_tri}.{file_extension}"
        blob = bucket.blob(filename)

        try:
            print(f"Downloading: {rec['link']}")
            response = requests.get(rec["link"], stream=True)
            response.raise_for_status()
            buffer = io.BytesIO()
            for chunk in response.iter_content(chunk_size=8192):
                buffer.write(chunk)
            buffer.seek(0)

            blob.upload_from_file(buffer)
            print(f"Uploaded {filename} to bucket {bucket.name}")

            extract_and_upload(buffer, file_year, file_tri, file_extension, bucket)

            new_entries.append(rec)
        except requests.exceptions.RequestException as e:
            print(f"Failed to download {filename}: {e}")
        except Exception as e:
            print(f"Error uploading {filename}: {e}")

    if new_entries:
        updated_history = history + new_entries
        write_history_to_gcp(bucket, updated_history)


def process_dataset(page_url: str, base_resource: str, bucket_name: str):
    html = fetch_html_content(page_url)
    links = extract_filtered_links(html, base_resource)

    results = []
    for link in links:
        filename = link.split("/")[-1]
        data = parse_filename(filename)
        if data:
            data["link"] = link
            results.append(data)
        else:
            print("Could not parse filename:", filename)

    results = deduplicate_results(results)
    results = sort_results(results)

    print("Found", len(results), "files:")
    for rec in results:
        print(
            f"{rec['trimestre']:>9} | {rec['year']} | {rec['month']:>5} | {rec['link']}"
        )

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name.strip())
    upload_files_to_gcp(results, bucket)


def main():
    page_url = "https://www.data.gouv.fr/fr/datasets/le-marche-du-haut-et-tres-haut-debit-fixe-deploiements/"
    base_resource = "https://static.data.gouv.fr/resources/le-marche-du-haut-et-tres-haut-deploiements"
    bucket_name = "ofr-2kt-valo-fibre"
    process_dataset(page_url, base_resource, bucket_name)


if __name__ == "__main__":
    main()
