import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "CommonCode"))

import logutil
import uvicorn
from datetime import datetime
from app.config import cfg
from app.database import get_status

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
        f"  KORELIBRARY  [{now}]",
        sep,
        "",
        row("Host:", f"http://{host}:{port}/"),
        row("Data dir:", data_dir),
        row("Log level:", log_level),
        row("Total books:", str(stats["total_books"])),
        row("Incomplete records:", str(stats["incomplete_records"])),
        row("Books without body:", str(stats["books_without_body"])),
        "",
        sep,
        "",
    ]
    print("\n".join(lines))


if __name__ == "__main__":
    from app.database import init_db
    _DATA_DIR = Path(cfg["data_dir"])
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    init_db()
    _print_banner()
    uvicorn.run(
        "app.api:app",
        host=cfg["host"],
        port=cfg["port"],
        log_level=cfg["log_level"],
        log_config=logutil.make_log_config(_DATA_DIR / "service.log"),
    )
