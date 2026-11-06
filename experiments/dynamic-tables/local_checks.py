"""Offline checks of exact statistics and cross-account ownership protection."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import admin
import common
from dt_oracle import self_check


class LocalChecks(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.identity = {'organization': 'example_org', 'account': 'example_account', 'region': 'example_region'}
        self.owned = {'identity': self.identity, 'database': common.DB, 'warehouse': common.DT_WH}
        self.write_manifest()

    def tearDown(self):
        self.directory.cleanup()

    def write_manifest(self):
        (self.root / 'owned_resources.json').write_text(json.dumps(self.owned))

    def test_independent_membership_and_precision_oracle(self):
        self.assertEqual(self_check()['status'], 'passed')

    def test_wrong_account_blocks_mutations(self):
        other = dict(self.identity, account='another_account')
        with patch.object(common, 'EVIDENCE', self.root), patch.object(admin, 'EVIDENCE', self.root), \
             patch.object(common, 'account_identity', return_value=other), \
             patch.object(admin, 'connect', return_value=MagicMock()), patch.object(admin, 'query') as query, \
             patch('sys.argv', ['admin.py', 'cleanup']):
            with self.assertRaisesRegex(RuntimeError, 'differs'):
                admin.main()
            query.assert_not_called()

    def test_modifying_stage_requires_full_ownership(self):
        self.owned['warehouse'] = None
        self.write_manifest()
        with patch.object(common, 'EVIDENCE', self.root), \
             patch.object(common, 'account_identity', return_value=self.identity):
            with self.assertRaisesRegex(RuntimeError, 'does not currently own'):
                common.verify_ownership(object())

    def test_drop_clears_ownership_and_repeat_cannot_drop_reused_name(self):
        recorded = []
        def fake_query(conn, sql, *args, **kwargs):
            recorded.append(sql)
            return {'columns': ['name'], 'rows': []}
        with patch.object(common, 'EVIDENCE', self.root), patch.object(admin, 'EVIDENCE', self.root), \
             patch.object(common, 'account_identity', return_value=self.identity), \
             patch.object(admin, 'connect', return_value=MagicMock()), \
             patch.object(admin, 'query', side_effect=fake_query), patch('sys.argv', ['admin.py', 'cleanup']):
            admin.main()
            cleared = json.loads((self.root / 'owned_resources.json').read_text())
            self.assertIsNone(cleared['database'])
            self.assertIsNone(cleared['warehouse'])
            self.assertEqual(len([sql for sql in recorded if sql.startswith('DROP ')]), 2)
            recorded.clear()
            admin.main()
            self.assertEqual(recorded, [])


if __name__ == '__main__':
    unittest.main()
