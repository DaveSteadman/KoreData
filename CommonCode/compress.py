import zlib
from typing import Optional


def compress(text: Optional[str]) -> Optional[bytes]:
    if not text:
        return None
    return zlib.compress(text.encode("utf-8"), level=6)


def decompress(blob: Optional[bytes]) -> Optional[str]:
    if not blob:
        return None
    if isinstance(blob, str):
        return blob  # legacy uncompressed row
    return zlib.decompress(blob).decode("utf-8")
