from config import load_config

_SECTION = "korefeed"

_DEFAULTS = {
    "port": 8801,
    "host": "0.0.0.0",
    "data_dir": "../Data/Feeds",
    "log_level": "info",
}

cfg = load_config(_SECTION, _DEFAULTS)
