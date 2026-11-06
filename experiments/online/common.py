"""Original standalone configuration and evidence helpers for this experiment."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import threading

from online_session import connection_config

ROOT = Path(__file__).resolve().parent
EVIDENCE = Path(os.environ.get("TS_LAB_EVIDENCE_DIR", str(ROOT / "evidence"))).expanduser().resolve()
PROFILE = os.environ.get("SNOWFLAKE_CONNECTION_NAME")
_LOCK = threading.Lock()


def identifier(variable: str, default: str | None = None, max_length: int = 255) -> str | None:
    value = os.environ.get(variable, default)
    if value is None:
        return None
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) or len(value) > max_length:
        raise ValueError(f"{variable} must be an unquoted Snowflake identifier of at most {max_length} characters")
    return value.upper()


DB = identifier("SNOWFLAKE_DATABASE", "TIMESERIES_ONLINE_LAB")
SCHEMA = identifier("SNOWFLAKE_SCHEMA", "ONLINE_LAB", max_length=42)
FS_WH = identifier("SNOWFLAKE_WAREHOUSE", "TIMESERIES_ONLINE_WH")
PRODUCER_ROLE = identifier("SNOWFLAKE_PRODUCER_ROLE")
CONSUMER_ROLE = identifier("SNOWFLAKE_CONSUMER_ROLE")
CLIENT_LOCATION = os.environ.get("SNOWFLAKE_CLIENT_LOCATION", "Unspecified client location; timings include network RTT")


def config():
    return connection_config(PROFILE)


def append(name, obj):
    with _LOCK:
        EVIDENCE.mkdir(parents=True, exist_ok=True)
        with (EVIDENCE / name).open("a") as output:
            output.write(json.dumps(obj, default=str) + "\n")


def dump(name, obj):
    with _LOCK:
        EVIDENCE.mkdir(parents=True, exist_ok=True)
        target = EVIDENCE / name
        temporary = target.with_suffix(target.suffix + ".tmp")
        temporary.write_text(json.dumps(obj, indent=2, default=str) + "\n")
        temporary.replace(target)
