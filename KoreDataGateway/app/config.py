import json
from pathlib import Path

from config import load_config

_CONFIG_FILE = Path("../config/default.json")
_SECTION = "koredatagateway"

# Keys in ports{} that map to gateway URL settings
_SVC_URLS = {
    "korefeed": "korefeed_url",
    "korelibrary": "korelibrary_url",
    "korerag": "korerag_url",
    "korereference": "korereference_url",
}

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
    result = load_config(_SECTION, _DEFAULTS)
    # Add service URL mappings derived from ports config.
    # Explicit section-level overrides (already applied by load_config) take priority.
    if _CONFIG_FILE.exists():
        with open(_CONFIG_FILE, encoding="utf-8") as f:
            raw = json.load(f)
        section_overrides = raw.get(_SECTION, {})
        for svc, url_key in _SVC_URLS.items():
            if svc in raw.get("ports", {}) and url_key not in section_overrides:
                result[url_key] = f"http://127.0.0.1:{raw['ports'][svc]}"
    return result


cfg = load()
