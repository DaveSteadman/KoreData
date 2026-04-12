import json
from pathlib import Path

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
    if _CONFIG_FILE.exists():
        with open(_CONFIG_FILE, encoding="utf-8") as f:
            raw = json.load(f)
        result = dict(_DEFAULTS)
        for key in ("host", "log_level"):
            if key in raw:
                result[key] = raw[key]
        ports = raw.get("ports", {})
        if _SECTION in ports:
            result["port"] = ports[_SECTION]
        for svc, url_key in _SVC_URLS.items():
            if svc in ports:
                result[url_key] = f"http://127.0.0.1:{ports[svc]}"
        result.update(raw.get(_SECTION, {}))
        return result
    return dict(_DEFAULTS)


cfg = load()
