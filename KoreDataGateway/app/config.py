import json
from pathlib import Path

_CONFIG_FILE = Path("config/default.json")

_DEFAULTS = {
    "port": 8800,
    "host": "0.0.0.0",
    "log_level": "info",
    "korefeed_url": "http://127.0.0.1:8801",
    "korelibrary_url": "http://127.0.0.1:8802",
    "korereference_url": "http://127.0.0.1:8804",
    "korerag_url": "http://127.0.0.1:8803",
}


def load() -> dict:
    if _CONFIG_FILE.exists():
        with open(_CONFIG_FILE, encoding="utf-8") as f:
            return {**_DEFAULTS, **json.load(f)}
    return dict(_DEFAULTS)


cfg = load()
