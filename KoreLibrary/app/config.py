import json
from pathlib import Path

_CONFIG_FILE = Path("config/default.json")

_DEFAULTS = {
    "port": 8802,
    "host": "0.0.0.0",
    "data_dir": "data",
    "log_level": "info",
    "kiwix_url": "http://127.0.0.1:8888",
}


def load() -> dict:
    if _CONFIG_FILE.exists():
        with open(_CONFIG_FILE, encoding="utf-8") as f:
            return {**_DEFAULTS, **json.load(f)}
    return dict(_DEFAULTS)


cfg = load()
