"""Original minimal key-pair helper; no copied Cortex Code skill implementation.

Reads one explicitly selected Snowflake CLI profile. Credentials are never printed.
Python 3.12 is the tested local runtime. No automatic selection of another profile.
"""
from __future__ import annotations

import os
from pathlib import Path
import tomllib

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey


def connection_config(profile: str | None = None) -> dict:
    selected = profile or os.environ.get("SNOWFLAKE_CONNECTION_NAME")
    if not selected:
        raise ValueError("Set SNOWFLAKE_CONNECTION_NAME to the explicit CLI connection profile for this experiment")
    snowflake_directory = Path(os.environ.get("SNOWFLAKE_HOME", "~/.snowflake")).expanduser()
    path = Path(os.environ.get("SNOWFLAKE_CONNECTIONS_FILE", str(snowflake_directory / "connections.toml"))).expanduser()
    profiles = tomllib.loads(path.read_text())
    if selected not in profiles or not isinstance(profiles[selected], dict):
        raise ValueError("The explicitly selected Snowflake profile was not found in connections.toml")
    source = profiles[selected]
    required = ("account", "user", "private_key_file")
    if any(not isinstance(source.get(name), str) or not source[name] for name in required):
        raise ValueError("The selected profile must contain account, user and private_key_file")
    if str(source.get("authenticator", "SNOWFLAKE_JWT")).upper() != "SNOWFLAKE_JWT":
        raise ValueError("This REST experiment supports key-pair authentication (SNOWFLAKE_JWT) only")
    result = {name: source[name] for name in required}
    result["private_key_file"] = str(Path(result["private_key_file"]).expanduser())
    result["authenticator"] = "SNOWFLAKE_JWT"
    if source.get("role"):
        result["role"] = source["role"]
    return result


def load_private_key(settings: dict) -> RSAPrivateKey:
    password = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    key = serialization.load_pem_private_key(
        Path(settings["private_key_file"]).read_bytes(),
        password=password.encode() if password is not None else None,
    )
    if not isinstance(key, RSAPrivateKey):
        raise ValueError("The REST JWT implementation requires an RSA private key")
    return key


def create_snowpark_session(profile: str | None = None):
    from snowflake.snowpark import Session

    settings = connection_config(profile)
    key = load_private_key(settings)
    parameters = {name: settings[name] for name in ("account", "user", "authenticator", "role") if name in settings}
    parameters["private_key"] = key.private_bytes(
        serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    parameters.update(login_timeout=30, network_timeout=120)
    return Session.builder.configs(parameters).create()
