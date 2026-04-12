import json
from pathlib import Path

_CONFIG_FILE = Path("../config/default.json")
_SECTION = "korelibrary"

_DEFAULTS = {
    "port": 8802,
    "host": "0.0.0.0",
    "data_dir": "../Data/Library",
    "log_level": "info",
}


def load() -> dict:
    if _CONFIG_FILE.exists():
        with open(_CONFIG_FILE, encoding="utf-8") as f:
            raw = json.load(f)
        result = dict(_DEFAULTS)
        for key in ("host", "log_level"):
            if key in raw:
                result[key] = raw[key]
        port = raw.get("ports", {}).get(_SECTION)
        if port is not None:
            result["port"] = port
        result.update(raw.get(_SECTION, {}))
        return result
    return dict(_DEFAULTS)


cfg = load()
