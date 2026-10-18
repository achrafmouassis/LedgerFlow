"""Fast unit checks; no HTTP traffic or database required."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch
import tempfile
from types import SimpleNamespace
from decimal import Decimal
from io import BytesIO

spec = importlib.util.spec_from_file_location('qa_runner', Path(__file__).with_name('run.py'))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class RunnerTests(unittest.TestCase):
    def test_nearest_rank_percentiles(self):
        self.assertEqual(0, runner.percentile([], .95))
        self.assertEqual(95, runner.percentile(list(range(1, 101)), .95))
        self.assertEqual(50, runner.percentile(list(range(1, 101)), .50))

    def test_refuses_application_database_before_connecting(self):
        with patch.dict(runner.os.environ, {'QA_ISOLATED': '1', 'PGDATABASE': 'ledgerflow'}):
            with self.assertRaisesRegex(RuntimeError, 'isolated'):
                runner.Campaign(None)

    def test_requires_explicit_isolation_flag(self):
        with patch.dict(runner.os.environ, {'QA_ISOLATED': '0', 'PGDATABASE': 'ledgerflow_qa'}):
            with self.assertRaises(RuntimeError):
                runner.Campaign(None)

    def test_network_failure_is_recorded_as_ambiguous_not_success(self):
        campaign = runner.Campaign.__new__(runner.Campaign)
        campaign.lock = runner.threading.Lock()
        with tempfile.TemporaryDirectory() as directory:
            campaign.directory = Path(directory)
            with patch.object(runner, 'urlopen', side_effect=runner.URLError('offline')):
                result = campaign.request('POST', '/transfers', {'amount': '1'}, 'retry-key')
            self.assertEqual(0, result['status'])
            record = runner.json.loads((Path(directory) / 'requests.jsonl').read_text())
            self.assertEqual('retry-key', record['key'])
            self.assertEqual(0, record['status'])

    def test_load_request_buffer_flushes_without_losing_records(self):
        campaign = runner.Campaign.__new__(runner.Campaign)
        campaign.lock = runner.threading.Lock()
        campaign.buffered_requests = []
        campaign.args = SimpleNamespace(phase='scale-1', p95_ms=2000)
        campaign.run_id, campaign.started = 'test', '2026-10-17'
        campaign.checks, campaign.stages = [], []
        with tempfile.TemporaryDirectory() as directory:
            campaign.directory = Path(directory)
            with patch.object(runner, 'urlopen', side_effect=runner.URLError('offline')):
                for i in range(257):
                    campaign.request('GET', f'/test/{i}')
            records = Path(directory) / 'requests.jsonl'
            self.assertEqual(256, len(records.read_text().splitlines()))
            campaign.report()
            self.assertEqual(257, len(records.read_text().splitlines()))
            self.assertEqual([], campaign.buffered_requests)

    def test_report_retains_failures_skips_and_escapes_html(self):
        campaign = runner.Campaign.__new__(runner.Campaign)
        campaign.args = SimpleNamespace(phase='full', p95_ms=2000)
        campaign.run_id, campaign.started = 'test', '2026-10-16'
        campaign.checks = [dict(name='<script>', status='FAIL', detail=Decimal('0.10')),
                           dict(name='Batch', status='SKIP', detail='not run')]
        campaign.stages = []
        with tempfile.TemporaryDirectory() as directory:
            campaign.directory = Path(directory)
            campaign.report()
            result = runner.json.loads((Path(directory) / 'report.json').read_text())
            self.assertEqual(['FAIL', 'SKIP'], [check['status'] for check in result['checks']])
            content = (Path(directory) / 'report.html').read_text(encoding='utf-8')
            self.assertIn('&lt;script&gt;', content)
            self.assertNotIn('<script>', content)

    def test_http_error_is_preserved_with_business_response(self):
        campaign = runner.Campaign.__new__(runner.Campaign)
        campaign.lock = runner.threading.Lock()
        error = runner.HTTPError('http://backend', 409, 'Conflict', {},
                                 BytesIO(b'{"code":"INSUFFICIENT_FUNDS"}'))
        with tempfile.TemporaryDirectory() as directory:
            campaign.directory = Path(directory)
            with patch.object(runner, 'urlopen', side_effect=error):
                result = campaign.cash('account', '80', 'withdrawals', 'same-key')
            self.assertEqual(409, result['status'])
            self.assertEqual('INSUFFICIENT_FUNDS', result['response']['code'])

    def test_response_amounts_keep_decimal_precision(self):
        campaign = runner.Campaign.__new__(runner.Campaign)
        campaign.lock = runner.threading.Lock()
        response = unittest.mock.MagicMock()
        response.__enter__.return_value = response
        response.status = 200
        response.read.return_value = b'{"balance":99999999999999999.99}'
        with tempfile.TemporaryDirectory() as directory:
            campaign.directory = Path(directory)
            with patch.object(runner, 'urlopen', return_value=response):
                result = campaign.request('GET', '/accounts/account')
            self.assertEqual(Decimal('99999999999999999.99'), result['response']['balance'])


if __name__ == '__main__':
    unittest.main()
