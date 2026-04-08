import json
from pathlib import Path

_CONFIG_FILE = Path("config/default.json")

_DEFAULTS = {
    "port": 8000,
    "host": "0.0.0.0",
    "data_dir": "data",
    "feeds_dir": "feeds",
    "log_level": "info",
}


def load() -> dict:
    if _CONFIG_FILE.exists():
        with open(_CONFIG_FILE, encoding="utf-8") as f:
            return {**_DEFAULTS, **json.load(f)}
    return dict(_DEFAULTS)


# Module-level singleton so other modules can do: from app.config import cfg
cfg = load()
