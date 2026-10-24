"""Entrypoint tests for disabled, bootstrap and enabled controller modes."""
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import renovate_policy


class EntrypointTests(unittest.TestCase):
    def test_disabled_without_credentials_or_repository_files_is_a_noop(self):
        script = Path(renovate_policy.__file__).resolve()
        with tempfile.TemporaryDirectory() as directory:
            for enabled in (None, '', 'false', 'TRUE', 'True', '1'):
                with self.subTest(enabled=enabled):
                    env = {} if enabled is None else {'RENOVATE_POLICY_ENABLED': enabled}
                    result = subprocess.run([sys.executable, str(script)], cwd=directory,
                                            env=env, capture_output=True, text=True, timeout=5)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn('disabled', result.stdout)

    def test_disabled_with_credentials_never_creates_api_client_or_reads_inputs(self):
        env = {'RENOVATE_POLICY_TOKEN': 'policy-placeholder',
               'GITHUB_TOKEN': 'read-placeholder'}
        with patch.dict(os.environ, env, clear=True), patch.object(sys, 'argv', ['controller']), \
                patch.object(renovate_policy, 'GitHub') as api, \
                patch.object(Path, 'read_text') as read, contextlib.redirect_stdout(io.StringIO()):
            renovate_policy.main()
        api.assert_not_called()
        read.assert_not_called()

    def test_enabled_requires_merge_credential_even_with_github_token(self):
        env = {'RENOVATE_POLICY_ENABLED': 'true', 'GITHUB_TOKEN': 'read-placeholder'}
        with patch.dict(os.environ, env, clear=True), patch.object(sys, 'argv', ['controller']), \
                patch.object(renovate_policy, 'GitHub') as api:
            with self.assertRaisesRegex(ValueError, 'GITHUB_TOKEN is not a merge credential'):
                renovate_policy.main()
        api.assert_not_called()

    def test_bootstrap_requires_explicit_merge_credential_while_disabled(self):
        with patch.dict(os.environ, {'GITHUB_TOKEN': 'read-placeholder'}, clear=True), \
                patch.object(sys, 'argv', ['controller', '--bootstrap']), \
                patch.object(renovate_policy, 'GitHub') as api:
            with self.assertRaisesRegex(ValueError, 'RENOVATE_POLICY_TOKEN is required'):
                renovate_policy.main()
        api.assert_not_called()

    def test_bootstrap_is_independent_of_enabled_and_skips_reconciliation(self):
        for enabled in ('', 'true'):
            env = {'RENOVATE_POLICY_ENABLED': enabled, 'RENOVATE_POLICY_TOKEN': 'policy-placeholder',
                   'GITHUB_REPOSITORY': 'fin-assist/example'}
            with self.subTest(enabled=enabled), patch.dict(os.environ, env, clear=True), \
                    patch.object(sys, 'argv', ['controller', '--bootstrap']), \
                    patch.object(renovate_policy, 'GitHub') as api, \
                    patch.object(renovate_policy, 'reconcile') as reconcile:
                renovate_policy.main()
                api.assert_called_once_with('policy-placeholder', 'fin-assist/example')
                api.return_value.bootstrap.assert_called_once_with()
                reconcile.assert_not_called()

    def test_enabled_passes_current_event_and_tokens_to_guarded_reconciliation(self):
        config = {'requiredChecks': ['go'], 'controllerLogin': 'controller'}
        event = {'workflow_run': {'id': 12, 'head_sha': 'current-sha'}}
        for name in ('workflow_run', 'workflow_dispatch', 'schedule'):
            env = {'RENOVATE_POLICY_ENABLED': 'true', 'RENOVATE_POLICY_TOKEN': 'policy-placeholder',
                   'GITHUB_READ_TOKEN': 'read-placeholder', 'GITHUB_REPOSITORY': 'fin-assist/example',
                   'GITHUB_EVENT_PATH': 'event.json', 'GITHUB_EVENT_NAME': name}
            with self.subTest(name=name), patch.dict(os.environ, env, clear=True), \
                    patch.object(sys, 'argv', ['controller']), \
                    patch.object(Path, 'read_text', side_effect=[json.dumps(config), json.dumps(event)]), \
                    patch.object(renovate_policy, 'GitHub') as api, \
                    patch.object(renovate_policy, 'reconcile') as reconcile:
                renovate_policy.main()
                api.assert_called_once_with('policy-placeholder', 'fin-assist/example', 'read-placeholder')
                reconcile.assert_called_once_with(api.return_value, config,
                                                  event['workflow_run'] if name == 'workflow_run' else None)
