"""Local ownership guards using fake account identities; no cloud calls."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import online_capability as capability


IDENTITY = {"ORGANIZATION": "EXAMPLE_ORG", "ACCOUNT_LOCATOR": "EXAMPLE_LOCATOR", "REGION": "EXAMPLE_REGION"}


class OwnershipChecks(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="online-owner-check-")
        self.addCleanup(temporary.cleanup)
        self.manifest = Path(temporary.name) / "owner.json"
        self.owner = {
            "locator": capability.LOCATOR, "run_id": capability.DB, "account_identity": deepcopy(IDENTITY),
            "status_before": {"status": "NOT_FOUND"}, "create_attempted": True,
            "created": False, "cleaned": False,
        }
        self.save()
        for name, value in (("MANIFEST", self.manifest), ("PRODUCER_ROLE", "LAB_PRODUCER"), ("CONSUMER_ROLE", "LAB_CONSUMER")):
            patcher = patch.object(capability, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        for name in ("get_status", "feature_store", "append", "dump"):
            patcher = patch.object(capability, name)
            setattr(self, name, patcher.start())
            self.addCleanup(patcher.stop)
        patcher = patch.object(capability, "account_identity", return_value=deepcopy(IDENTITY))
        self.identity = patcher.start()
        self.addCleanup(patcher.stop)

    def save(self):
        self.manifest.write_text(json.dumps(self.owner))

    def test_matching_live_identity(self):
        capability.verify_owner(None, self.owner)
        self.identity.assert_called_once_with(None)

    def test_missing_live_identity_fails(self):
        self.owner.pop("account_identity")
        with self.assertRaises(RuntimeError):
            capability.verify_owner(None, self.owner)

    def test_different_account_fails_before_status_or_drop(self):
        self.identity.return_value = dict(IDENTITY, ACCOUNT_LOCATOR="OTHER_LOCATOR")
        with self.assertRaises(RuntimeError):
            capability.cleanup(None)
        self.get_status.assert_not_called()
        self.feature_store.assert_not_called()

    def test_locator_change_is_not_owned(self):
        self.owner["locator"] = "OTHER_DB.OTHER_SCHEMA"
        self.save()
        with self.assertRaises(RuntimeError):
            capability.cleanup(None)
        self.feature_store.assert_not_called()

    def test_cleaned_manifest_preserves_new_service(self):
        self.owner["cleaned"] = True
        self.save()
        self.get_status.return_value = {"status": "RUNNING"}
        with self.assertRaises(RuntimeError):
            capability.cleanup(None)
        self.feature_store.assert_not_called()

    def test_cleaned_and_absent_is_idempotent(self):
        self.owner["cleaned"] = True
        self.save()
        self.get_status.return_value = {"status": "NOT_FOUND"}
        self.assertEqual(capability.cleanup(None)["status"], "CLEAN")
        self.feature_store.assert_not_called()

    def test_unproven_initial_absence_cannot_drop(self):
        self.owner["status_before"] = {"status": "RUNNING"}
        self.save()
        with self.assertRaises(RuntimeError):
            capability.cleanup(None)
        self.get_status.assert_not_called()
        self.feature_store.assert_not_called()

    def test_cross_account_retry_is_refused(self):
        self.identity.return_value = dict(IDENTITY, ACCOUNT_LOCATOR="OTHER_LOCATOR")
        with self.assertRaises(RuntimeError):
            capability.setup(None, retry=True)
        self.get_status.assert_not_called()
        self.feature_store.assert_not_called()

    def test_no_manifest_preserves_service(self):
        self.manifest.unlink()
        self.assertEqual(capability.cleanup(None)["status"], "NO_MANIFEST_PRESERVED")
        self.get_status.assert_not_called()
        self.feature_store.assert_not_called()

    def test_existing_service_cannot_be_claimed(self):
        self.manifest.unlink()
        self.get_status.return_value = {"status": "RUNNING"}
        with self.assertRaises(RuntimeError):
            capability.setup(None)
        self.dump.assert_not_called()
        self.feature_store.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=1)
