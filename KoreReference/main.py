import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "CommonCode"))

import logutil
import uvicorn
from datetime import datetime
from app.config import cfg

_W = 80


def _print_banner() -> None:
    now = datetime.now().strftime("%H:%M:%S")
    sep = "=" * _W

    def row(label: str, value: str) -> str:
        return f"  {label:<24} {value}"

    lines = [
        "",
        sep,
        f"  KOREREFERENCE  [{now}]",
        sep,
        "",
        row("Host:", f"http://{cfg['host']}:{cfg['port']}/"),
        row("Data dir:", cfg["data_dir"]),
        row("Log level:", cfg["log_level"].upper()),
        "",
        sep,
        "",
    ]
    print("\n".join(lines))


if __name__ == "__main__":
    from app.database import init_db, get_status
    _DATA_DIR = Path(cfg["data_dir"])
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    init_db()
    stats = get_status()
    _print_banner()
    uvicorn.run(
        "app.api:app",
        host=cfg["host"],
        port=cfg["port"],
        log_level=cfg["log_level"],
        log_config=logutil.make_log_config(_DATA_DIR / "service.log"),
    )
