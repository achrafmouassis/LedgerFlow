"""Synthetic banking QA. Refuses any database other than the dedicated QA DB.

No third-party HTTP/load-test dependency. SQL uses the existing psycopg package.
Reports retain failed and ambiguous requests; an HTTP timeout is never a success.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import datetime, timezone
from decimal import Decimal
import html
import json
import math
import os
from pathlib import Path
import random
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import uuid

from ledgerflow_batch.config import DatabaseSettings, DateRange

BASE = os.environ.get('QA_API_BASE', 'http://backend:8080/api')


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else 0


class Campaign:
    def __init__(self, args):
        if os.environ.get('QA_ISOLATED') != '1' or os.environ.get('PGDATABASE') != 'ledgerflow_qa':
            raise RuntimeError('Only the isolated ledgerflow_qa environment is allowed')
        self.args = args
        self.run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '-' + uuid.uuid4().hex[:8]
        self.directory = Path('/reports') / self.run_id
        self.directory.mkdir(parents=True)
        self.lock = threading.Lock()
        self.checks, self.stages, self.accounts = [], [], []
        self.expected = {}
        self.db = DatabaseSettings.from_env().connect()
        self.started = datetime.now(timezone.utc).isoformat()
        self.token = None
        response = self.request('POST', '/auth/login', {'username': os.environ.get('QA_USERNAME', 'admin'), 'password': os.environ['QA_PASSWORD']}, sensitive=True)
        if response['status'] != 200:
            raise RuntimeError('QA authentication failed')
        self.token = response['response']['token']

    def check(self, name, condition, detail=''):
        self.checks.append(dict(name=name, status='PASS' if condition else 'FAIL', detail=str(detail)))
        print(f"{self.checks[-1]['status']}: {name}", flush=True)

    def request(self, method, path, payload=None, key=None, sensitive=False):
        headers = {'Content-Type': 'application/json'}
        if getattr(self, 'token', None):
            headers['Authorization'] = 'Bearer ' + self.token
        if key is not None:
            headers['Idempotency-Key'] = key
        data = json.dumps(payload).encode() if payload is not None else None
        start = time.perf_counter()
        upstream = None
        try:
            with urlopen(Request(BASE + path, data=data, headers=headers, method=method), timeout=20) as response:
                status, body = response.status, response.read().decode()
                upstream = response.headers.get('X-QA-Upstream')
        except HTTPError as error:
            status, body = error.code, error.read().decode()
            upstream = error.headers.get('X-QA-Upstream')
        except (URLError, TimeoutError, OSError) as error:
            status, body = 0, type(error).__name__
        elapsed = (time.perf_counter() - start) * 1000
        try:
            result = json.loads(body, parse_float=Decimal)
        except (ValueError, TypeError):
            result = {'raw': body[:500]}
        record = dict(at=datetime.now(timezone.utc).isoformat(), group=getattr(self, 'group', 'functional'),
                      method=method, path=path, key=key, payload=payload, status=status,
                      elapsed_ms=elapsed, response=result, upstream=upstream)
        if not sensitive:
            line = json.dumps(record, default=str) + '\n'
            with self.lock:
                if hasattr(self, 'buffered_requests'):
                    self.buffered_requests.append(line)
                    if len(self.buffered_requests) >= 256:
                        self.flush_requests()
                else:
                    with (self.directory / 'requests.jsonl').open('a', encoding='utf-8') as stream:
                        stream.write(line)
        return record

    def flush_requests(self):
        if not getattr(self, 'buffered_requests', None):
            return
        with (self.directory / 'requests.jsonl').open('a', encoding='utf-8') as stream:
            stream.writelines(self.buffered_requests)
        self.buffered_requests.clear()

    def account(self, currency='MAD', balance='0'):
        names = ['Amine Demo', 'Sara Demo', 'Epicerie Demo', 'Loyer Demo', 'Entreprise Demo']
        response = self.request('POST', '/accounts', dict(owner=f'QA-{self.run_id}-{names[len(self.accounts) % 5]}',
                                                         currency=currency, openingBalance=balance))
        if response['status'] != 201:
            raise RuntimeError('Account creation failed; see request manifest')
        account = response['response']['id']
        self.accounts.append(account)
        self.expected[account] = Decimal(balance)
        return account

    def cash(self, account, amount, kind='deposits', key=None):
        return self.request('POST', f'/accounts/{account}/{kind}', {'amount': str(amount)}, key or uuid.uuid4().hex)

    def scalar(self, sql, params=()):
        return self.db.execute(sql, params).fetchone()[0]

    def functional(self):
        self.pairs = [(self.account(c), self.account(c)) for c in ('MAD', 'EUR', 'USD')]
        for a, b in self.pairs:
            response = self.cash(a, '5000')
            self.check('Initial deposit', response['status'] == 200)
            if response['status'] == 200:
                self.expected[a] += Decimal('5000')
            response = self.cash(a, '25.50', 'withdrawals')
            self.check('Withdrawal', response['status'] == 200)
            if response['status'] == 200:
                self.expected[a] -= Decimal('25.50')
            response = self.request('POST', '/transfers', dict(sourceAccountId=a, destinationAccountId=b, amount='100.25'), uuid.uuid4().hex)
            self.check('Same-currency transfer', response['status'] == 200)
            if response['status'] == 200:
                self.expected[a] -= Decimal('100.25')
                self.expected[b] += Decimal('100.25')
        a, b = self.pairs[0]
        key = uuid.uuid4().hex
        first = self.cash(a, '12.34', key=key)
        replay = self.cash(a, '12.34', key=key)
        self.check('Identical retry returns same operation', first['status'] == replay['status'] == 200 and first['response'] == replay['response'])
        if first['status'] == 200:
            self.expected[a] += Decimal('12.34')
        before = self.scalar('SELECT count(*) FROM journals')
        cases = [('Same key, different amount', f'/accounts/{a}/deposits', {'amount': '13'}, key, 409),
                 ('Insufficient funds', f'/accounts/{b}/withdrawals', {'amount': '900000'}, uuid.uuid4().hex, 409),
                 ('Missing key', f'/accounts/{a}/deposits', {'amount': '1'}, None, 400),
                 ('Invalid key', f'/accounts/{a}/deposits', {'amount': '1'}, 'invalid key', 400),
                 ('Unknown account', f'/accounts/{uuid.uuid4()}/deposits', {'amount': '1'}, uuid.uuid4().hex, 404),
                 ('Same account', '/transfers', dict(sourceAccountId=a, destinationAccountId=a, amount='1'), uuid.uuid4().hex, 400),
                 ('Different currencies', '/transfers', dict(sourceAccountId=a, destinationAccountId=self.pairs[1][0], amount='1'), uuid.uuid4().hex, 400),
                 ('Blank owner', '/accounts', dict(owner=' ', currency='MAD', openingBalance='0'), None, 400),
                 ('Invalid currency', '/accounts', dict(owner='Demo', currency='mad', openingBalance='0'), None, 400),
                 ('Negative opening balance', '/accounts', dict(owner='Demo', currency='MAD', openingBalance='-1'), None, 400),
                 ('Missing amount', f'/accounts/{a}/deposits', {}, uuid.uuid4().hex, 400),
                 ('Null amount', f'/accounts/{a}/deposits', {'amount': None}, uuid.uuid4().hex, 400)]
        for value in ('0', '-1', '1.001', '100000000000000000'):
            cases.append((f'Invalid amount {value}', f'/accounts/{a}/deposits', {'amount': value}, uuid.uuid4().hex, 400))
        for name, path, payload, request_key, expected in cases:
            result = self.request('POST', path, payload, request_key)
            self.check(name, result['status'] == expected, f"HTTP {result['status']}, expected {expected}")
        self.check('Rejected operations create no journal', self.scalar('SELECT count(*) FROM journals') == before)
        key = uuid.uuid4().hex
        with ThreadPoolExecutor(8) as pool:
            results = list(pool.map(lambda _: self.cash(a, '7.89', key=key), range(8)))
        self.check('8 concurrent identical requests execute once', all(r['status'] == 200 for r in results) and
                   len({r['response'].get('id') for r in results}) == 1)
        # Independent expectation is one effect, even if the implementation is buggy.
        self.expected[a] += Decimal('7.89')
        poor = self.account(balance='100')
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(lambda _: self.cash(poor, '80', 'withdrawals'), range(2)))
        self.check('Concurrent spending cannot overdraw', sorted(r['status'] for r in results) == [200, 409])
        self.expected[poor] = Decimal('20')
        self.check('Invalid history pagination', self.request('GET', f'/accounts/{a}/journal?size=101')['status'] == 400)
        self.check('Invalid account identifier', self.request('GET', '/accounts/not-a-uuid')['status'] == 400)
        self.check('Reversed analytics period', self.request('GET', '/analytics/daily?from=2026-02-02&to=2026-02-01')['status'] == 400)

    def load(self):
        rng = random.Random(42)
        # Dedicated funded accounts per worker avoid confusing a hot-account lock with whole-system capacity.
        for workers in self.args.concurrency:
            self.group = f'load-{workers}'
            accounts = [self.account(('MAD', 'EUR', 'USD')[i % 3], '100000000') for i in range(workers)]
            work = [(accounts[i % workers], ('deposits', 'withdrawals')[i % 2],
                     Decimal(rng.randint(100, 2500)) / 100, uuid.uuid4().hex) for i in range(self.args.operations)]
            start = time.perf_counter()
            with ThreadPoolExecutor(workers) as pool:
                results = list(pool.map(lambda item: self.cash(item[0], item[2], item[1], item[3]), work))
            elapsed = time.perf_counter() - start
            for (account, kind, amount, _), result in zip(work, results):
                if result['status'] == 200:
                    self.expected[account] += amount if kind == 'deposits' else -amount
            latencies = [r['elapsed_ms'] for r in results]
            errors = sum(r['status'] != 200 for r in results)
            stage = dict(concurrency=workers, requests=len(results), errors=errors, seconds=elapsed,
                         requests_per_second=len(results) / elapsed, p50_ms=percentile(latencies, .50),
                         p95_ms=percentile(latencies, .95), p99_ms=percentile(latencies, .99))
            self.stages.append(stage)
            self.check(f'Load {workers} workers: no unexpected error', errors == 0, stage)
            self.check(f'Load {workers} workers: p95 budget', stage['p95_ms'] <= self.args.p95_ms, stage['p95_ms'])
        self.group = 'reconciliation'

    def reconcile(self):
        for account, expected in self.expected.items():
            actual = self.request('GET', f'/accounts/{account}')
            self.check(f'Expected balance {account}', actual['status'] == 200 and Decimal(str(actual['response'].get('balance', '-1'))) == expected,
                       f'expected {expected}, actual {actual["response"].get("balance")}')
        mismatch = self.scalar('''SELECT count(*) FROM accounts a WHERE a.balance IS DISTINCT FROM
            (SELECT coalesce(sum(CASE WHEN j.credit_account_id=a.id THEN j.amount ELSE -j.amount END),0)
             FROM journals j WHERE j.credit_account_id=a.id OR j.debit_account_id=a.id)''')
        self.check('All stored balances match journal reconstruction', mismatch == 0, mismatch)
        self.check('Every journal has an Outbox event', self.scalar('''SELECT count(*) FROM journals j
            LEFT JOIN outbox_events o ON o.journal_id=j.id WHERE o.event_id IS NULL''') == 0)
        self.check('Journal and Outbox content match', self.scalar('''SELECT count(*) FROM journals j
            JOIN outbox_events o ON o.journal_id=j.id WHERE
            (j.type,j.debit_account_id,j.credit_account_id,j.amount,j.currency,j.created_at) IS DISTINCT FROM
            (o.operation_type,o.debit_account_id,o.credit_account_id,o.amount,o.currency,o.occurred_at)''') == 0)
        sql = '''SELECT count(*) FROM outbox_events o
            LEFT JOIN analytics.processed_events p USING(event_id)
            LEFT JOIN analytics.transaction_facts f USING(event_id)
            WHERE o.published_at IS NULL OR p.event_id IS NULL OR f.event_id IS NULL OR
            (o.journal_id,o.operation_type,o.debit_account_id,o.credit_account_id,o.amount,o.currency,o.occurred_at)
            IS DISTINCT FROM
            (f.journal_id,f.operation_type,f.debit_account_id,f.credit_account_id,f.amount,f.currency,f.occurred_at)'''
        start = time.monotonic()
        missing = self.scalar(sql)
        while missing and time.monotonic() - start < self.args.drain_seconds:
            time.sleep(2)
            missing = self.scalar(sql)
        self.check('Every Outbox event published and processed with identical content', missing == 0,
                   dict(missing_or_different=missing, drain_seconds=time.monotonic() - start))
        self.check('No extra analytical event', self.scalar('''SELECT count(*) FROM analytics.transaction_facts f
            LEFT JOIN outbox_events o USING(event_id) WHERE o.event_id IS NULL''') == 0)
        self.check('No negative account balance', self.scalar('SELECT count(*) FROM accounts WHERE balance<0') == 0)
        today = datetime.now(timezone.utc).date()
        daily = self.request('GET', f'/analytics/daily?from={today}&to={today}')
        expected = self.db.execute('''SELECT event_date,currency,operation_type,count(*),sum(amount)
            FROM analytics.transaction_facts WHERE event_date=%s GROUP BY 1,2,3 ORDER BY 1,2,3''', (today,)).fetchall()
        actual = sorted((r['date'], r['currency'], r['operationType'], r['operations'], Decimal(str(r['totalAmount'])))
                        for r in daily['response']) if daily['status'] == 200 else []
        self.check('Streaming statistics API matches exact SQL', daily['status'] == 200 and actual ==
                   [(str(day), currency, kind, count, amount) for day, currency, kind, count, amount in expected])

    def batch(self):
        if self.args.skip_batch:
            self.checks.append(dict(name='Spark batch, retry and BI reconciliation', status='SKIP', detail='--skip-batch'))
            return
        from ledgerflow_batch.job import run_batch
        period = self.db.execute("SELECT min(event_date),max(event_date) FROM analytics.transaction_facts").fetchone()
        if period[0] is None:
            raise RuntimeError('No facts available for batch')
        settings = DatabaseSettings.from_env()
        date_range = DateRange(*period)
        columns = 'event_date,currency,operation_type,operation_count,total_amount,min_amount,max_amount'
        results = []
        for attempt in range(2):
            result = run_batch(settings, date_range)
            self.check(f'Spark execution {attempt + 1}', result['status'] == 'SUCCEEDED', result)
            results.append(self.db.execute(f'SELECT {columns} FROM analytics.daily_operation_summary ORDER BY 1,2,3').fetchall())
        expected = self.db.execute('''SELECT event_date,currency,operation_type,count(*),sum(amount),min(amount),max(amount)
            FROM analytics.transaction_facts GROUP BY 1,2,3 ORDER BY 1,2,3''').fetchall()
        self.check('Batch exact counts/sums/min/max equal SQL source', results[0] == expected)
        self.check('Batch replay produces identical results', results[0] == results[1])
        rows = self.db.execute('SELECT * FROM analytics.bi_daily_operations ORDER BY 1,2,3')
        with (self.directory / 'bi_daily_operations.csv').open('w', newline='', encoding='utf-8-sig') as stream:
            writer = csv.writer(stream)
            writer.writerow([column.name for column in rows.description])
            writer.writerows(rows.fetchall())
        self.check('BI view exposes every summary row', self.scalar('SELECT count(*) FROM analytics.bi_daily_operations') == len(expected))

    def outage(self):
        account = self.account()
        key = uuid.uuid4().hex
        result = self.cash(account, '123.45', key=key)
        self.check('Banking write accepted while Kafka is stopped', result['status'] == 200)
        row = self.db.execute('''
            SELECT o.event_id,o.published_at FROM outbox_events o JOIN journals j ON j.id=o.journal_id
            WHERE j.idempotency_key=%s''', (key,)).fetchone()
        self.check('Outbox retains unpublished event', row is not None and row[1] is None)
        if row:
            Path('/reports/outage.json').write_text(json.dumps(dict(event_id=str(row[0]), account=account, key=key)), encoding='utf-8')

    def recovery(self):
        probe = json.loads(Path('/reports/outage.json').read_text(encoding='utf-8'))
        start = time.monotonic()
        while time.monotonic() - start < self.args.drain_seconds:
            count = self.scalar('SELECT count(*) FROM analytics.transaction_facts WHERE event_id=%s AND amount=123.45', (probe['event_id'],))
            if count:
                break
            time.sleep(2)
        self.check('After Kafka restart, pending event reaches analytics exactly once', count == 1)
        response = self.cash(probe['account'], '123.45', key=probe['key'])
        self.check('Retry after recovery remains idempotent', response['status'] == 200 and
                   self.scalar('SELECT balance FROM accounts WHERE id=%s', (probe['account'],)) == Decimal('123.45'))

    def report(self):
        if hasattr(self, 'buffered_requests'):
            with self.lock:
                self.flush_requests()
        result = dict(run_id=self.run_id, started=self.started, phase=self.args.phase,
                      synthetic_data=True, load_model=getattr(self, 'load_model', 'closed-loop bounded concurrency; not a maximum capacity proof'),
                      checks=self.checks, stages=self.stages, expected_balances=getattr(self, 'expected', {}))
        (self.directory / 'report.json').write_text(json.dumps(result, indent=2, default=str), encoding='utf-8')
        esc = lambda value: html.escape(str(value))
        rows = ''.join(f'<tr><td>{esc(c["status"])}</td><td>{esc(c["name"])}</td><td>{esc(c["detail"])}</td></tr>' for c in self.checks)
        bars = ''.join(f'<p>{s["concurrency"]} utilisateurs simultanés : {s["requests_per_second"]:.1f} requêtes/s ; '
                       f'p95 {s["p95_ms"]:.0f} ms ; erreurs {s["errors"]}</p>'
                       f'<div style="background:#287dba;height:20px;width:{min(100, s["p95_ms"] / self.args.p95_ms * 100):.1f}%"></div>' for s in self.stages)
        page = f'''<!doctype html><html lang="fr"><meta charset="utf-8"><title>LedgerFlow QA</title>
        <style>body{{font:16px system-ui;max-width:1200px;margin:40px auto;padding:20px;background:#f6f8fb}}
        td,th{{border:1px solid #ccc;padding:8px;text-align:left;overflow-wrap:anywhere}}table{{border-collapse:collapse;width:100%}}</style>
        <h1>LedgerFlow — rapport QA</h1><p>{esc(self.run_id)} — {esc(self.args.phase)}</p>
        <p>Données fictives. Environnement isolé. Les résultats mesurent cette machine, pas une capacité de production.</p>
        <h2>Performance</h2><p>Barre : p95 / budget de {self.args.p95_ms} ms. Débit réellement observé, sans objectif de débit imposé.</p>{bars}
        <h2>Vérifications</h2><table><tr><th>Résultat</th><th>Test</th><th>Détail</th></tr>{rows}</table>
        <p>Fichiers associés : report.json, requests.jsonl ; export BI si le batch a été exécuté.
        Les pannes non exécutées et contrôles manuels sont listés dans docs/qa.md.</p></html>'''
        (self.directory / 'report.html').write_text(page, encoding='utf-8')
        print(f'Report: {self.directory}/report.html', flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--operations', type=int, default=120)
    parser.add_argument('--concurrency', default='1,4,8')
    parser.add_argument('--p95-ms', type=int, default=2000)
    parser.add_argument('--drain-seconds', type=int, default=180)
    parser.add_argument('--skip-batch', action='store_true')
    parser.add_argument('--phase', choices=['full', 'outage', 'recovery'], default='full')
    args = parser.parse_args()
    args.concurrency = [int(x) for x in args.concurrency.split(',')]
    if not 1 <= args.operations <= 100000 or not args.concurrency or any(not 1 <= n <= 64 for n in args.concurrency) or args.p95_ms <= 0 or args.drain_seconds <= 0:
        parser.error('operations 1..100000, concurrency 1..64, positive budgets required')
    campaign = Campaign(args)
    try:
        if args.phase == 'full':
            campaign.functional()
            campaign.load()
            campaign.reconcile()
            campaign.batch()
        else:
            getattr(campaign, args.phase)()
    except Exception as error:
        campaign.check('Campaign completed without unexpected exception', False, type(error).__name__)
        raise
    finally:
        campaign.report()
        campaign.db.close()
    return int(any(c['status'] == 'FAIL' for c in campaign.checks))


if __name__ == '__main__':
    raise SystemExit(main())
