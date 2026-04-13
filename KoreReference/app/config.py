from config import load_config

_SECTION = "korereference"

_DEFAULTS = {
    "port": 8804,
    "host": "0.0.0.0",
    "data_dir": "../Data/Reference",
    "log_level": "info",
}

cfg = load_config(_SECTION, _DEFAULTS)
