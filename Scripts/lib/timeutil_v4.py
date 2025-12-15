# Scripts/lib/timeutil_v4.py
from __future__ import annotations
import shutil
from pathlib import Path
from datetime import datetime
import zoneinfo

NY = zoneinfo.ZoneInfo("America/New_York")

def now_ny() -> datetime:
    return datetime.now(tz=NY)

def ts_ny() -> str:
    return now_ny().strftime("%Y%m%d_%H%M")

def backup_with_timestamp(src: Path, backup_dir: Path, prefix: str | None = None) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{prefix}_" if prefix else ""
    ts = ts_ny()
    out = backup_dir / f"{stem}{ts}{src.suffix}"
    shutil.copy2(src, out)
    return out

def newest(path_glob: str) -> Path | None:
    paths = sorted(Path().glob(path_glob))
    return paths[-1] if paths else None
