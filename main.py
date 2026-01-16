import json
from collections import OrderedDict
from pathlib import Path


def 统计错误日志(input_path: Path) -> dict:
    """读取单个JSON文件并统计错误日志。"""
    with input_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    logs = data.get("logs", [])
    计数 = {}
    total_errors = 0

    for item in logs:
        if not isinstance(item, dict):
            continue
        if item.get("level") != "ERROR":
            continue
        service = item.get("service")
        if not service:
            continue
        计数[service] = 计数.get(service, 0) + 1
        total_errors += 1

    # 按字典序排序 error_stats
    error_stats = OrderedDict((k, 计数[k]) for k in sorted(计数))

    return {
        "error_stats": error_stats,
        "total_errors": total_errors,
    }


def main() -> None:
    root = Path(__file__).resolve().parent
    input_dir = root / "input"
    output_dir = root / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        return

    for input_file in sorted(input_dir.glob("*.json")):
        result = 统计错误日志(input_file)
        output_file = output_dir / input_file.name
        with output_file.open("w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
