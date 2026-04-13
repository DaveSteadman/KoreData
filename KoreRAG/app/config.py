from config import load_config

_SECTION = "korerag"

_DEFAULTS = {
    "port": 8803,
    "host": "0.0.0.0",
    "log_level": "info",
    "data_dir": "../Data/RAG",
}

cfg = load_config(_SECTION, _DEFAULTS)
