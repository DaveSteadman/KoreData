from config import load_config

_SECTION = "korelibrary"

_DEFAULTS = {
    "port": 8802,
    "host": "0.0.0.0",
    "data_dir": "../Data/Library",
    "log_level": "info",
}

cfg = load_config(_SECTION, _DEFAULTS)
