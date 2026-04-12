import json
from pathlib import Path

_CONFIG_FILE = Path("config/default.json")

_DEFAULTS = {
    "port": 8803,
    "host": "0.0.0.0",
    "log_level": "info",
    "data_dir": "data",
}


def load() -> dict:
    if _CONFIG_FILE.exists():
        with open(_CONFIG_FILE, encoding="utf-8") as f:
            return {**_DEFAULTS, **json.load(f)}
    return dict(_DEFAULTS)


cfg = load()
