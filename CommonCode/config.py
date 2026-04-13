import json
from pathlib import Path

_CONFIG_FILE = Path("../config/default.json")


def load_config(section: str, defaults: dict) -> dict:
    """Load config from default.json merging global and section-level settings.

    Resolution order (later wins):
      1. *defaults*
      2. Top-level ``host`` / ``log_level`` keys
      3. ``ports.<section>`` mapped to ``port``
      4. ``<section>`` dict (section-level overrides)
    """
    if not _CONFIG_FILE.exists():
        return dict(defaults)
    with open(_CONFIG_FILE, encoding="utf-8") as f:
        raw = json.load(f)
    result = dict(defaults)
    for key in ("host", "log_level"):
        if key in raw:
            result[key] = raw[key]
    port = raw.get("ports", {}).get(section)
    if port is not None:
        result["port"] = port
    result.update(raw.get(section, {}))
    return result
