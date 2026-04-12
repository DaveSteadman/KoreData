import logging
from pathlib import Path

_MAX_LINES = 1000


class LineCappedFileHandler(logging.FileHandler):
    """FileHandler that trims the log file to the last _MAX_LINES lines on each write."""

    def emit(self, record: logging.LogRecord) -> None:
        super().emit(record)
        self._trim()

    def _trim(self) -> None:
        path = Path(self.baseFilename)
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
        except OSError:
            return
        if len(lines) > _MAX_LINES:
            path.write_text("".join(lines[-_MAX_LINES:]), encoding="utf-8")


def make_log_config(log_path: str | Path) -> dict:
    """Return a uvicorn log_config dict that writes to *log_path* with line capping."""
    path = str(log_path)
    return {
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
            "default": {
                "()": "logutil.LineCappedFileHandler",
                "filename": path,
                "formatter": "default",
            },
            "access": {
                "()": "logutil.LineCappedFileHandler",
                "filename": path,
                "formatter": "access",
            },
        },
        "loggers": {
            "uvicorn":        {"handlers": ["default"], "level": "INFO", "propagate": False},
            "uvicorn.error":  {"handlers": ["default"], "level": "INFO", "propagate": False},
            "uvicorn.access": {"handlers": ["access"],  "level": "INFO", "propagate": False},
        },
    }
