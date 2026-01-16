import json
import os
import sys
from typing import Dict, List, Any


def aggregate_error_logs(logs: List[Dict[str, Any]]) -> Dict[str, Any]:
    counts: Dict[str, int] = {}
    total_errors = 0

    for log in logs:
        if not isinstance(log, dict):
            continue
        if log.get("level") != "ERROR":
            continue
        service = log.get("service")
        if not service:
            continue
        counts[service] = counts.get(service, 0) + 1
        total_errors += 1

    error_stats = {key: counts[key] for key in sorted(counts.keys())}
    return {"error_stats": error_stats, "total_errors": total_errors}


def read_json_from_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def read_json_from_stdin() -> Dict[str, Any]:
    content = sys.stdin.read()
    content = content.strip()
    if not content:
        return {"logs": []}
    return json.loads(content)


def write_output(path: str, data: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def process_file(input_path: str, output_path: str) -> None:
    data = read_json_from_file(input_path)
    logs = data.get("logs", [])
    if not isinstance(logs, list):
        logs = []
    result = aggregate_error_logs(logs)
    write_output(output_path, result)


def process_stdin(output_path: str) -> None:
    data = read_json_from_stdin()
    logs = data.get("logs", [])
    if not isinstance(logs, list):
        logs = []
    result = aggregate_error_logs(logs)
    write_output(output_path, result)


def main() -> None:
    input_dir = "input"
    output_dir = "output"

    if len(sys.argv) > 1:
        input_path = sys.argv[1]
        if os.path.isdir(input_path):
            for filename in os.listdir(input_path):
                if not filename.lower().endswith(".json"):
                    continue
                file_path = os.path.join(input_path, filename)
                name, _ = os.path.splitext(filename)
                output_filename = f"{name}-output.json"
                output_path = os.path.join(output_dir, output_filename)
                process_file(file_path, output_path)
        else:
            base_name = os.path.basename(input_path)
            name, _ = os.path.splitext(base_name)
            output_filename = f"{name}-output.json"
            output_path = os.path.join(output_dir, output_filename)
            if os.path.isfile(input_path):
                process_file(input_path, output_path)
            else:
                process_stdin(output_path)
        return

    for filename in os.listdir(input_dir):
        if not filename.lower().endswith(".json"):
            continue
        input_path = os.path.join(input_dir, filename)
        name, _ = os.path.splitext(filename)
        output_filename = f"{name}-output.json"
        output_path = os.path.join(output_dir, output_filename)
        process_file(input_path, output_path)


if __name__ == "__main__":
    main()
