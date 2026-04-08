import os
import uvicorn
from datetime import datetime
from app.config import cfg
from app.version import __version__

_LOG_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": "%(levelprefix)s %(message)s",
            "use_colors": False,
        },
        "access": {
            "()": "uvicorn.logging.AccessFormatter",
            "fmt": '%(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',
            "use_colors": False,
        },
    },
    "handlers": {
        "default": {"class": "logging.FileHandler", "filename": "data/gateway.log", "formatter": "default"},
        "access":  {"class": "logging.FileHandler", "filename": "data/gateway.log", "formatter": "access"},
    },
    "loggers": {
        "uvicorn":        {"handlers": ["default"], "level": "INFO", "propagate": False},
        "uvicorn.error": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "uvicorn.access":{"handlers": ["access"],  "level": "INFO", "propagate": False},
    },
}

_W = 80


def _print_banner() -> None:
    now = datetime.now().strftime("%H:%M:%S")
    sep = "=" * _W

    def row(label: str, value: str) -> str:
        return f"  {label:<24} {value}"

    lines = [
        "",
        sep,
        f"  KOREDATAGATEWAY {__version__}  [{now}]",
        sep,
        "",
        row("Gateway:", f"http://localhost:{cfg['port']}/"),
        row("KoreFeed:", cfg["korefeed_url"]),
        row("KoreLibrary:", cfg["korelibrary_url"]),
        row("KoreReference:", cfg["korereference_url"]),
        row("Log level:", cfg["log_level"].upper()),
        "",
        sep,
        "",
    ]
    print("\n".join(lines))


if __name__ == "__main__":
    _print_banner()
    os.makedirs("data", exist_ok=True)
    uvicorn.run(
        "app.api:app",
        host=cfg["host"],
        port=cfg["port"],
        log_level=cfg["log_level"],
        log_config=_LOG_CONFIG,
    )
