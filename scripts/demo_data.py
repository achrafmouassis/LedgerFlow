"""Explicitly authorized synthetic main-app dataset, using banking API only.

SQL is read-only except for a session advisory lock. Stable labels and keys make
an interrupted injection resumable without duplicating accounts or operations.
"""
import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import html
import json
import os
from pathlib import Path
import random
import re
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from ledgerflow_batch.config import DatabaseSettings, DateRange

LOCK = 83864682876239
BASE = 'http://backend:8080/api'


def money(cents):
    return f'{cents // 100}.{cents % 100:02d}'


def make_plan(dataset, count=120, operations=6000, seed=20261016):
    if not re.fullmatch(r'[a-z0-9-]{1,40}', dataset) or count < 12 or operations < 30:
        raise ValueError('Invalid dataset or insufficient sample size')
    rng = random.Random(seed)
    currencies = ['MAD'] * 5 + ['EUR'] * 3 + ['USD'] * 2
    accounts, balances, grouped = [], [], defaultdict(list)
    for index in range(count):
        currency = currencies[index % 10]
        role = 'Commerce' if index % 4 == 0 else 'Particulier'
        opening = rng.randint(100000, 1200000) if currency == 'MAD' else rng.randint(20000, 250000)
        accounts.append(dict(owner=f'DEMO | {dataset} | {role} {index + 1:03d} | {currency}',
                             currency=currency, opening_cents=opening, role=role))
        balances.append(opening)
        grouped[currency].append(index)
    planned = []
    for index in range(operations):
        source = rng.randrange(count)
        currency = accounts[source]['currency']
        kind = rng.choices(['DEPOSIT', 'WITHDRAWAL', 'TRANSFER'], [25, 25, 50])[0]
        if kind == 'DEPOSIT':
            amount = rng.randint(50000, 450000) if currency == 'MAD' else rng.randint(10000, 120000)
            destination = source
            debit = None
            label = 'Approvisionnement / revenu fictif'
        elif kind == 'WITHDRAWAL':
            amount = rng.randint(2000, 40000) if currency == 'MAD' else rng.randint(500, 12000)
            destination, debit = None, source
            label = 'Retrait especes fictif'
        else:
            candidates = [a for a in grouped[currency] if a != source]
            merchants = [a for a in candidates if accounts[a]['role'] == 'Commerce']
            destination = rng.choice(merchants if merchants and rng.random() < .7 else candidates)
            debit = source
            amount = rng.randint(1000, 25000) if currency == 'MAD' else rng.randint(500, 8000)
            label = 'Achat / reglement fictif' if accounts[destination]['role'] == 'Commerce' else 'Virement entre clients fictifs'
        # Keep a safety reserve. Low funds become a real API deposit, never an overdraft.
        if debit is not None and balances[debit] < amount + 1000:
            kind, destination, debit, label = 'DEPOSIT', source, None, 'Reapprovisionnement fictif'
        if debit is not None:
            balances[debit] -= amount
        if destination is not None:
            balances[destination] += amount
        planned.append(dict(key=f'{dataset}:{index:06d}', kind=kind, debit=debit,
                            credit=destination, cents=amount, currency=currency, scenario=label))
    plan = dict(dataset=dataset, synthetic=True, seed=seed, accounts=accounts,
                operations=planned, expected_balance_cents=balances)
    validate_plan(plan)
    return plan


def validate_plan(plan):
    accounts = plan['accounts']
    balances = [a['opening_cents'] for a in accounts]
    totals = defaultdict(int)
    for a in accounts:
        totals[a['currency']] += a['opening_cents']
    keys = set()
    for op in plan['operations']:
        if op['key'] in keys or op['cents'] <= 0:
            raise ValueError('Duplicate key or non-positive amount')
        keys.add(op['key'])
        debit, credit = op['debit'], op['credit']
        if op['kind'] == 'TRANSFER' and (debit is None or credit is None or debit == credit):
            raise ValueError('Invalid transfer')
        if op['kind'] == 'DEPOSIT' and (debit is not None or credit is None):
            raise ValueError('Invalid deposit')
        if op['kind'] == 'WITHDRAWAL' and (debit is None or credit is not None):
            raise ValueError('Invalid withdrawal')
        if op['kind'] not in ('TRANSFER', 'DEPOSIT', 'WITHDRAWAL'):
            raise ValueError('Unknown operation')
        for account in (debit, credit):
            if account is not None and accounts[account]['currency'] != op['currency']:
                raise ValueError('Currency mismatch')
        if debit is not None:
            balances[debit] -= op['cents']
            if balances[debit] < 0:
                raise ValueError('Overdraft in plan')
        else:
            totals[op['currency']] += op['cents']
        if credit is not None:
            balances[credit] += op['cents']
        else:
            totals[op['currency']] -= op['cents']
    if balances != plan['expected_balance_cents']:
        raise ValueError('Expected balances do not match independent reconstruction')
    actual = defaultdict(int)
    for a, balance in zip(accounts, balances):
        actual[a['currency']] += balance
    if actual != totals:
        raise ValueError('Currency-level funds conservation failed')


class Injection:
    def __init__(self, plan, directory, max_rps=20):
        self.plan, self.directory = plan, directory
        self.checks = []
        self.max_rps = max_rps
        self.settings = DatabaseSettings.from_env()
        if self.settings.database != 'ledgerflow':
            raise ValueError('Explicit main-app injection requires PGDATABASE=ledgerflow')
        self.db = self.settings.connect()
        if not self.db.execute('SELECT pg_try_advisory_lock(%s)', (LOCK,)).fetchone()[0]:
            raise RuntimeError('Another demo injection is active')
        self.ids = []
        credentials = dict(username=os.environ.get('LEDGERFLOW_USERNAME','admin'), password=os.environ['LEDGERFLOW_ADMIN_PASSWORD'])
        with urlopen(Request(BASE+'/auth/login',data=json.dumps(credentials).encode(),headers={'Content-Type':'application/json'}),timeout=30) as response:
            self.token=json.load(response)['token']

    def check(self, name, condition, detail=''):
        self.checks.append(dict(name=name, status='PASS' if condition else 'FAIL', detail=str(detail)))
        print(f'{self.checks[-1]["status"]}: {name}', flush=True)
        if not condition:
            raise RuntimeError(name)

    def api(self, path, payload, key=None):
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer '+self.token}
        if key:
            headers['Idempotency-Key'] = key
        request = Request(BASE + path, data=json.dumps(payload).encode(), headers=headers, method='POST')
        try:
            with urlopen(request, timeout=30) as response:
                status, content = response.status, response.read().decode()
        except HTTPError as error:
            status, content = error.code, error.read().decode()
        except (URLError, TimeoutError, OSError) as error:
            status, content = 0, json.dumps(dict(error=type(error).__name__))
        result = json.loads(content, parse_float=Decimal)
        with (self.directory / 'requests.jsonl').open('a', encoding='utf-8') as stream:
            stream.write(json.dumps(dict(at=datetime.now(timezone.utc).isoformat(), path=path,
                key=key, payload=payload, status=status, response=result), default=str) + '\n')
        if status not in (200, 201):
            raise RuntimeError(f'HTTP {status}; stop and resume with the SAME dataset; inspect requests.jsonl')
        return result

    def inject(self):
        for account in self.plan['accounts']:
            rows = self.db.execute('SELECT id,currency FROM accounts WHERE owner=%s', (account['owner'],)).fetchall()
            if len(rows) > 1:
                raise RuntimeError('Ambiguous account label; manual review required')
            if rows:
                identifier, currency = rows[0]
                if currency != account['currency']:
                    raise RuntimeError('Existing demo account currency mismatch')
                opening = self.db.execute("SELECT amount FROM journals WHERE type='OPENING' AND credit_account_id=%s", (identifier,)).fetchall()
                if opening != [(Decimal(money(account['opening_cents'])),)]:
                    raise RuntimeError('Existing demo account opening does not match plan')
                self.ids.append(str(identifier))
            else:
                # Account creation has no idempotency key. A timeout STOPS the script;
                # resume first looks up the exact stable label instead of blindly retrying.
                result = self.api('/accounts', dict(owner=account['owner'], currency=account['currency'],
                                                  openingBalance=money(account['opening_cents'])))
                self.ids.append(result['id'])
        print(f'Accounts ready: {len(self.ids)}', flush=True)
        existing = {row[0]: row[1:] for row in self.db.execute('''SELECT idempotency_key,type,debit_account_id,
            credit_account_id,amount,currency FROM journals WHERE idempotency_key LIKE %s''', (self.plan['dataset'] + ':%',))}
        allowed_keys = {op['key'] for op in self.plan['operations']}
        if set(existing) - allowed_keys:
            raise RuntimeError('Existing operations outside the plan; do not change dataset parameters')
        for index, op in enumerate(self.plan['operations']):
            debit = self.ids[op['debit']] if op['debit'] is not None else None
            credit = self.ids[op['credit']] if op['credit'] is not None else None
            if op['key'] in existing:
                kind, d, c, amount, currency = existing[op['key']]
                if (kind, str(d) if d else None, str(c) if c else None, amount, currency) != (
                        op['kind'], debit, credit, Decimal(money(op['cents'])), op['currency']):
                    raise RuntimeError('Existing operation differs from plan')
                continue
            if op['kind'] == 'TRANSFER':
                path, payload = '/transfers', dict(sourceAccountId=debit, destinationAccountId=credit, amount=money(op['cents']))
            else:
                identifier = credit or debit
                path = f'/accounts/{identifier}/' + ('deposits' if credit else 'withdrawals')
                payload = dict(amount=money(op['cents']))
            request_started = time.monotonic()
            self.api(path, payload, op['key'])
            time.sleep(max(0, 1 / self.max_rps - (time.monotonic() - request_started)))
            if (index + 1) % 500 == 0:
                print(f'Operations accepted: {index + 1}/{len(self.plan["operations"])}', flush=True)
        self.check('Planned account count', len(self.ids) == len(self.plan['accounts']))

    def verify(self):
        rows = self.db.execute('SELECT id,balance FROM accounts WHERE id=ANY(%s::uuid[])', (self.ids,)).fetchall()
        stored = {str(identifier): amount for identifier, amount in rows}
        self.check('Every account balance equals the independently planned result',
            all(stored.get(identifier) == Decimal(money(expected)) for identifier, expected in zip(self.ids, self.plan['expected_balance_cents'])))
        actual = self.db.execute('''SELECT idempotency_key,type,debit_account_id,credit_account_id,amount,currency
            FROM journals WHERE idempotency_key LIKE %s''', (self.plan['dataset'] + ':%',)).fetchall()
        expected = [(op['key'], op['kind'], self.ids[op['debit']] if op['debit'] is not None else None,
                     self.ids[op['credit']] if op['credit'] is not None else None, Decimal(money(op['cents'])), op['currency'])
                    for op in self.plan['operations']]
        actual = [(key, kind, str(d) if d else None, str(c) if c else None, amount, currency) for key, kind, d, c, amount, currency in actual]
        self.check('Every planned operation matches journal content exactly', sorted(actual) == sorted(expected))
        self.check('All application balances reconstruct from journal', self.db.execute('''SELECT count(*) FROM accounts a
            WHERE a.balance IS DISTINCT FROM (SELECT coalesce(sum(CASE WHEN j.credit_account_id=a.id THEN j.amount ELSE -j.amount END),0)
            FROM journals j WHERE j.credit_account_id=a.id OR j.debit_account_id=a.id)''').fetchone()[0] == 0)
        self.check('Journal and Outbox match exactly', self.db.execute('''SELECT count(*) FROM journals j
            FULL JOIN outbox_events o ON o.journal_id=j.id WHERE j.id IS NULL OR o.event_id IS NULL OR
            (j.type,j.debit_account_id,j.credit_account_id,j.amount,j.currency,j.created_at) IS DISTINCT FROM
            (o.operation_type,o.debit_account_id,o.credit_account_id,o.amount,o.currency,o.occurred_at)''').fetchone()[0] == 0)
        query = '''SELECT count(*) FROM outbox_events o LEFT JOIN analytics.processed_events p USING(event_id)
            FULL JOIN analytics.transaction_facts f USING(event_id) WHERE o.event_id IS NULL OR o.published_at IS NULL OR
            p.event_id IS NULL OR f.event_id IS NULL OR
            f.event_date IS DISTINCT FROM (o.occurred_at AT TIME ZONE 'UTC')::date OR
            (o.journal_id,o.operation_type,o.debit_account_id,o.credit_account_id,o.amount,o.currency,o.occurred_at)
            IS DISTINCT FROM (f.journal_id,f.operation_type,f.debit_account_id,f.credit_account_id,f.amount,f.currency,f.occurred_at)'''
        start = time.monotonic()
        missing = self.db.execute(query).fetchone()[0]
        while missing and time.monotonic() - start < 600:
            print(f'Pipeline catching up: {missing} events remaining', flush=True)
            time.sleep(10)
            missing = self.db.execute(query).fetchone()[0]
        self.check('Every event published and received with identical content', missing == 0, dict(remaining=missing, drain_seconds=time.monotonic()-start))
        self.check('No negative account balance', self.db.execute('SELECT count(*) FROM accounts WHERE balance<0').fetchone()[0] == 0)

    def batch(self):
        from ledgerflow_batch.job import run_batch
        period = self.db.execute('''SELECT min((created_at AT TIME ZONE 'UTC')::date),max((created_at AT TIME ZONE 'UTC')::date)
            FROM journals WHERE credit_account_id=ANY(%s::uuid[]) OR debit_account_id=ANY(%s::uuid[])''', (self.ids, self.ids)).fetchone()
        result = run_batch(self.settings, DateRange(*period))
        self.check('Spark summary published', result['status'] == 'SUCCEEDED', result)
        source = self.db.execute('''SELECT event_date,currency,operation_type,count(*),sum(amount),min(amount),max(amount)
            FROM analytics.transaction_facts WHERE event_date BETWEEN %s AND %s GROUP BY 1,2,3 ORDER BY 1,2,3''', period).fetchall()
        published = self.db.execute('''SELECT event_date,currency,operation_type,operation_count,total_amount,min_amount,max_amount
            FROM analytics.daily_operation_summary WHERE event_date BETWEEN %s AND %s ORDER BY 1,2,3''', period).fetchall()
        self.check('Spark counts, sums and min/max match exact SQL', source == published)
        self.check('BI view contains published groups', self.db.execute('SELECT count(*) FROM analytics.bi_daily_operations WHERE event_date BETWEEN %s AND %s', period).fetchone()[0] == len(source))
        self.batch_result = result

    def report(self):
        result = dict(dataset=self.plan['dataset'], synthetic=True, checks=self.checks,
                      account_count=len(self.ids), operation_count=len(self.plan['operations']),
                      by_type=dict(Counter(op['kind'] for op in self.plan['operations'])),
                      batch=getattr(self, 'batch_result', None))
        (self.directory / 'verification.json').write_text(json.dumps(result, indent=2, default=str), encoding='utf-8')
        rows = ''.join(f'<tr><td>{html.escape(c["status"])}</td><td>{html.escape(c["name"])}</td><td>{html.escape(c["detail"])}</td></tr>' for c in self.checks)
        page = f'''<!doctype html><html lang="fr"><meta charset="utf-8"><title>Donnees de demonstration LedgerFlow</title>
        <style>body{{font:16px system-ui;max-width:1100px;margin:40px auto;padding:20px}}td,th{{padding:10px;border:1px solid #ccc}}table{{border-collapse:collapse}}</style>
        <h1>LedgerFlow — donnees de demonstration</h1><p>Donnees entierement fictives, creees dans la base principale via l'API.</p>
        <p>Jeu {html.escape(self.plan['dataset'])} : {len(self.ids)} comptes, {len(self.plan['operations'])} operations plus les ouvertures.</p>
        <table><tr><th>Resultat</th><th>Verification</th><th>Detail</th></tr>{rows}</table></html>'''
        (self.directory / 'verification.html').write_text(page, encoding='utf-8')
        print(json.dumps(result, default=str), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default='demo-20261016-v1')
    parser.add_argument('--accounts', type=int, default=120)
    parser.add_argument('--operations', type=int, default=6000)
    parser.add_argument('--seed', type=int, default=20261016)
    parser.add_argument('--max-rps', type=int, default=20)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--confirm-database')
    parser.add_argument('--output', default='/reports')
    args = parser.parse_args()
    if not 1 <= args.max_rps <= 50:
        parser.error('--max-rps must be between 1 and 50')
    plan = make_plan(args.dataset, args.accounts, args.operations, args.seed)
    directory = Path(args.output) / args.dataset
    directory.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(plan, sort_keys=True, indent=2)
    path = directory / 'plan.json'
    if path.exists() and path.read_text(encoding='utf-8') != encoded:
        raise ValueError('Existing plan differs: do not change parameters of an injected dataset')
    path.write_text(encoded, encoding='utf-8')
    print(json.dumps(dict(plan_valid=True, accounts=len(plan['accounts']), operations=len(plan['operations']),
                         by_type=dict(Counter(op['kind'] for op in plan['operations'])),
                         sha256=hashlib.sha256(encoded.encode()).hexdigest())), flush=True)
    if not args.apply:
        return 0
    if args.confirm_database != 'ledgerflow':
        parser.error('Require --apply --confirm-database ledgerflow for main-app writes')
    job = Injection(plan, directory, args.max_rps)
    try:
        # Keep the baseline for diagnosis; never remove existing application data.
        baseline = directory / 'baseline.json'
        if not baseline.exists():
            rows = job.db.execute('SELECT id,owner,currency,balance FROM accounts ORDER BY id').fetchall()
            baseline.write_text(json.dumps(rows, default=str, indent=2), encoding='utf-8')
        job.inject()
        job.verify()
        job.batch()
    except Exception as error:
        job.checks.append(dict(name='Injection and verification completed', status='FAIL', detail=str(error)))
        raise
    finally:
        job.report()
        job.db.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
