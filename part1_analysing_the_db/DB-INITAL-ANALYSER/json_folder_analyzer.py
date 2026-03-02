import os
import json
import csv
from datetime import datetime

CUSTOMER_FOLDER = r"F:\WORK\adventure_dataset\numoni_final\databases\numoni_customer"
MERCHANT_FOLDER = r"F:\WORK\adventure_dataset\numoni_final\databases\numoni_merchant"

OUTPUT_FOLDER = r"F:\WORK\adventure_dataset\numoni_final\part1_analysing_the_db"

TXT_REPORT_FILE = os.path.join(OUTPUT_FOLDER, "db_analysis_report.txt")
JSON_REPORT_FILE = os.path.join(OUTPUT_FOLDER, "db_analysis_report.json")
CSV_REPORT_FILE = os.path.join(OUTPUT_FOLDER, "db_analysis_report.csv")


def get_json_summary(data):
    if isinstance(data, list):
        if len(data) == 0:
            return "JSON List (Empty list)"

        first_item = data[0]
        if isinstance(first_item, dict):
            keys = list(first_item.keys())
            return f"JSON List ({len(data)} records), first record keys: {keys[:8]}"
        return f"JSON List ({len(data)} records), first record type: {type(first_item).__name__}"

    if isinstance(data, dict):
        keys = list(data.keys())
        return f"JSON Object, keys: {keys[:12]}"

    return f"Unknown JSON format: {type(data).__name__}"


def analyze_json_file(file_path):
    file_size = os.path.getsize(file_path)

    if file_size == 0:
        return {
            "status": "EMPTY FILE",
            "summary": "File size is 0 bytes",
            "error": None,
            "file_size": file_size
        }

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            raw_text = f.read().strip()

            if raw_text == "":
                return {
                    "status": "EMPTY CONTENT",
                    "summary": "File contains only spaces/newlines",
                    "error": None,
                    "file_size": file_size
                }

            data = json.loads(raw_text)
            summary = get_json_summary(data)

            return {
                "status": "VALID",
                "summary": summary,
                "error": None,
                "file_size": file_size
            }

    except json.JSONDecodeError as e:
        return {
            "status": "CORRUPTED JSON",
            "summary": "JSON decoding failed",
            "error": f"{str(e)} (Line {e.lineno}, Column {e.colno})",
            "file_size": file_size
        }

    except UnicodeDecodeError as e:
        return {
            "status": "ENCODING ERROR",
            "summary": "Cannot read file in UTF-8 encoding",
            "error": str(e),
            "file_size": file_size
        }

    except Exception as e:
        return {
            "status": "UNKNOWN ERROR",
            "summary": "Unexpected error occurred",
            "error": str(e),
            "file_size": file_size
        }


def scan_folder(folder_path, db_name):
    results = []

    if not os.path.exists(folder_path):
        results.append({
            "db_name": db_name,
            "file_name": None,
            "status": "FOLDER NOT FOUND",
            "summary": f"Folder does not exist: {folder_path}",
            "error": None,
            "file_size": 0
        })
        return results

    json_files = [f for f in os.listdir(folder_path) if f.lower().endswith(".json")]

    if len(json_files) == 0:
        results.append({
            "db_name": db_name,
            "file_name": None,
            "status": "NO JSON FILES",
            "summary": "No JSON files found in folder",
            "error": None,
            "file_size": 0
        })
        return results

    for file in sorted(json_files):
        file_path = os.path.join(folder_path, file)
        report = analyze_json_file(file_path)

        results.append({
            "db_name": db_name,
            "file_name": file,
            "status": report["status"],
            "summary": report["summary"],
            "error": report["error"],
            "file_size": report["file_size"]
        })

    return results


def save_txt_report(all_results):
    with open(TXT_REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("NUMONI DATABASE ANALYSIS REPORT\n")
        f.write("=" * 100 + "\n")
        f.write(f"Generated on: {datetime.now()}\n")
        f.write("=" * 100 + "\n\n")

        for item in all_results:
            f.write(f"DB Name   : {item['db_name']}\n")
            f.write(f"File Name : {item['file_name']}\n")
            f.write(f"Status    : {item['status']}\n")
            f.write(f"Summary   : {item['summary']}\n")
            if item["error"]:
                f.write(f"Error     : {item['error']}\n")
            f.write(f"File Size : {item['file_size']} bytes\n")
            f.write("-" * 100 + "\n")


def save_json_report(all_results):
    with open(JSON_REPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=4)


def save_csv_report(all_results):
    with open(CSV_REPORT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["db_name", "file_name", "status", "summary", "error", "file_size_bytes"])

        for item in all_results:
            writer.writerow([
                item["db_name"],
                item["file_name"],
                item["status"],
                item["summary"],
                item["error"],
                item["file_size"]
            ])


if __name__ == "__main__":
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    all_results = []
    all_results.extend(scan_folder(CUSTOMER_FOLDER, "numoni_customer"))
    all_results.extend(scan_folder(MERCHANT_FOLDER, "numoni_merchant"))

    save_txt_report(all_results)
    save_json_report(all_results)
    save_csv_report(all_results)

    print("\n✅ Analysis Complete!")
    print(f"📄 TXT Report  : {TXT_REPORT_FILE}")
    print(f"📄 JSON Report : {JSON_REPORT_FILE}")
    print(f"📄 CSV Report  : {CSV_REPORT_FILE}")
