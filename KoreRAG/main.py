import uvicorn
from datetime import datetime
from app.config import cfg
from app.database import get_status
from app.version import __version__

_W = 80


def _print_banner() -> None:
    now = datetime.now().strftime("%H:%M:%S")
    stats = get_status()
    host = cfg["host"]
    port = cfg["port"]
    data_dir = cfg["data_dir"]
    log_level = cfg["log_level"].upper()

    sep = "=" * _W

    def row(label: str, value: str) -> str:
        return f"  {label:<22} {value}"

    lines = [
        "",
        sep,
        f"  KORERAG {__version__}  [{now}]",
        sep,
        "",
        row("Host:", f"http://{host}:{port}/"),
        row("Data dir:", data_dir),
        row("Log level:", log_level),
        row("Total chunks:", str(stats["total_chunks"])),
        row("DB size:", f"{stats['db_size_bytes']:,} bytes"),
        "",
        sep,
        "",
    ]
    print("\n".join(lines))


if __name__ == "__main__":
    from app.database import init_db
    init_db()
    _print_banner()
    uvicorn.run(
        "app.api:app",
        host=cfg["host"],
        port=cfg["port"],
        log_level=cfg["log_level"],
        reload=False,
    )
