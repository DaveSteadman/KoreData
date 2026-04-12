import threading

import_lock: threading.Lock = threading.Lock()
state_lock: threading.Lock = threading.Lock()
import_state: dict = {
    "running": False, "done": 0, "total": 0, "limit": 0, "errors": 0,
    "last_error": None, "mode": None, "seed": None,
    "redirects_stored": 0, "last_redirect": None,
}
