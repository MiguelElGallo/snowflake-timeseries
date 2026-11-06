"""Local authentication-routing checks using generated keys and fake profiles.

Never loads a real profile, contacts Snowflake, or writes a credential to evidence.
"""
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa

from common import identifier
from online_session import connection_config, create_snowpark_session, load_private_key


class SessionChecks(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="online-helper-check-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.config_path = self.root / "connections.toml"
        self.config_path.write_text(
            '[intended]\naccount="example-account"\nuser="EXAMPLE_USER"\n'
            'private_key_file="~/example-key.p8"\nrole="LAB_ROLE"\n'
            'password="ignored-fixture-only"\nwarehouse="IGNORED_WAREHOUSE"\n'
            '[other]\naccount="other-account"\nuser="OTHER_USER"\n'
            'private_key_file="~/other-key.p8"\n'
        )
        self.env = patch.dict("os.environ", {
            "SNOWFLAKE_CONNECTION_NAME": "intended",
            "SNOWFLAKE_CONNECTIONS_FILE": str(self.config_path),
        }, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_missing_selection_never_reads_a_profile(self):
        with patch.dict("os.environ", {}, clear=True), patch.object(Path, "read_text") as read:
            with self.assertRaises(ValueError):
                connection_config()
            read.assert_not_called()

    def test_exact_profile_and_only_supported_fields(self):
        settings = connection_config()
        self.assertEqual(settings["account"], "example-account")
        self.assertEqual(settings["role"], "LAB_ROLE")
        self.assertEqual(settings["authenticator"], "SNOWFLAKE_JWT")
        self.assertNotIn("password", settings)
        self.assertNotIn("warehouse", settings)
        self.assertFalse(settings["private_key_file"].startswith("~"))

    def test_explicit_profile_argument_wins(self):
        self.assertEqual(connection_config("other")["account"], "other-account")

    def test_unknown_profile_cannot_fall_back(self):
        with self.assertRaises(ValueError):
            connection_config("absent")

    def test_snowflake_home_selects_config_directory(self):
        with patch.dict("os.environ", {
            "SNOWFLAKE_CONNECTION_NAME": "intended", "SNOWFLAKE_HOME": str(self.root)
        }, clear=True):
            self.assertEqual(connection_config()["user"], "EXAMPLE_USER")

    def test_missing_required_key_is_rejected(self):
        self.config_path.write_text('[intended]\naccount="example-account"\nuser="EXAMPLE_USER"\n')
        with self.assertRaises(ValueError):
            connection_config()

    def test_non_keypair_authentication_is_rejected(self):
        with self.config_path.open("a") as output:
            output.write('authenticator="externalbrowser"\n')
        with self.assertRaises(ValueError):
            connection_config("other")

    def write_key(self, key, password=None):
        path = self.root / "generated-test-key.p8"
        encryption = serialization.BestAvailableEncryption(password) if password else serialization.NoEncryption()
        path.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, encryption))
        path.chmod(0o600)
        return {"private_key_file": str(path)}

    def test_unencrypted_rsa_key(self):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        loaded = load_private_key(self.write_key(key))
        self.assertEqual(loaded.public_key().public_numbers(), key.public_key().public_numbers())

    def test_encrypted_key_uses_explicit_passphrase_environment(self):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        settings = self.write_key(key, b"generated-test-passphrase")
        with patch.dict("os.environ", {"SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": "generated-test-passphrase"}):
            self.assertEqual(load_private_key(settings).key_size, 2048)

    def test_non_rsa_key_is_rejected(self):
        settings = self.write_key(ec.generate_private_key(ec.SECP256R1()))
        with self.assertRaises(ValueError):
            load_private_key(settings)

    def test_session_receives_der_and_no_key_file_or_password(self):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        parameters = {}
        marker = object()
        class Builder:
            def configs(self, values):
                parameters.update(values)
                return self
            def create(self):
                return marker
        fake = SimpleNamespace(Session=SimpleNamespace(builder=Builder()))
        with patch.dict("sys.modules", {"snowflake.snowpark": fake}), patch("online_session.load_private_key", return_value=key):
            self.assertIs(create_snowpark_session(), marker)
        self.assertEqual(parameters["account"], "example-account")
        self.assertNotIn("private_key_file", parameters)
        self.assertNotIn("password", parameters)
        self.assertEqual(serialization.load_der_private_key(parameters["private_key"], None).key_size, 2048)

    def test_identifiers_reject_injected_sql(self):
        with patch.dict("os.environ", {"TEST_IDENTIFIER": "LAB; DROP DATABASE OTHER"}):
            with self.assertRaises(ValueError):
                identifier("TEST_IDENTIFIER")
        with patch.dict("os.environ", {"TEST_IDENTIFIER": "lab_123"}):
            self.assertEqual(identifier("TEST_IDENTIFIER"), "LAB_123")


if __name__ == "__main__":
    unittest.main(verbosity=1)
