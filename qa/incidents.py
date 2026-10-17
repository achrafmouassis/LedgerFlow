"""Explicit probes for disposable ledgerflow_qa. Orchestrated by host script.
Never mounts or accesses a Docker socket and never targets the main database.
"""
import argparse
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import time
from uuid import uuid4
from run import Campaign

STATE=Path('/private/incident-state.json')

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('phase',choices=['prepare','backend-recovered','db-down','db-recovered','hold-stage','batch-running','batch-recovered','dlt'])
    args=parser.parse_args()
    if args.phase=='db-down':
        # No login possible during DB outage: reuse the token saved in private probe state.
        from run import BASE
        from urllib.error import HTTPError
        from urllib.request import Request,urlopen
        state=json.loads(STATE.read_text())
        try:
            with urlopen(Request(BASE+f'/accounts/{state["account"]}/deposits',
                headers={'Content-Type':'application/json','Authorization':'Bearer '+state['token'],'Idempotency-Key':state['db_key']},
                data=b'{"amount":"5.00"}'),timeout=15) as response:
                status=response.status
        except HTTPError as error: status=error.code
        if status!=503: raise AssertionError(f'Expected explicit HTTP 503, received {status}')
        print('PASS: DB outage returns HTTP 503, no false success',flush=True)
        Path('/reports/db-outage-result.json').write_text(json.dumps({'status':'PASS','http':status}))
        return 0
    campaign=Campaign(argparse.Namespace(phase='incident-'+args.phase,skip_batch=True,p95_ms=2000,drain_seconds=180))
    try:
        if args.phase=='prepare':
            account=campaign.account('EUR','0')
            key=uuid4().hex
            response=campaign.cash(account,'10',key=key)
            campaign.check('Committed banking operation before backend crash',response['status']==200)
            STATE.write_text(json.dumps(dict(account=account,key=key,event=response['response'],token=campaign.token,db_key=uuid4().hex),default=str))
        elif args.phase=='backend-recovered':
            state=json.loads(STATE.read_text());response=campaign.cash(state['account'],'10',key=state['key'])
            campaign.check('Retry after SIGKILL returns original operation',response['status']==200 and response['response']['id']==state['event']['id'])
            campaign.check('No double debit/credit after backend restart',campaign.scalar('SELECT balance FROM accounts WHERE id=%s',(state['account'],))==Decimal('10'))
            campaign.reconcile()
        elif args.phase=='db-recovered':
            state=json.loads(STATE.read_text())
            campaign.check('Unavailable-DB attempt wrote no journal',campaign.scalar('SELECT count(*) FROM journals WHERE idempotency_key=%s',(state['db_key'],))==0)
            response=campaign.cash(state['account'],'5',key=state['db_key'])
            campaign.check('Same-key retry after DB recovery succeeds once',response['status']==200 and campaign.scalar('SELECT balance FROM accounts WHERE id=%s',(state['account'],))==Decimal('15'))
            campaign.reconcile()
        elif args.phase=='hold-stage':
            with campaign.db.transaction():
                campaign.db.execute('LOCK TABLE analytics.daily_summary_stage IN ACCESS EXCLUSIVE MODE')
                Path('/reports/stage-lock-ready').write_text('ready')
                print('QA staging lock held; SIGTERM releases connection and lock.',flush=True)
                time.sleep(300)
        elif args.phase=='batch-running':
            deadline=time.monotonic()+90
            run=None
            while time.monotonic()<deadline:
                run=campaign.db.execute("SELECT run_id FROM analytics.batch_runs WHERE status='RUNNING' ORDER BY started_at DESC LIMIT 1").fetchone()
                if run: break
                time.sleep(.5)
            campaign.check('Real Spark batch reached RUNNING before kill',run is not None)
            state=json.loads(STATE.read_text());state['interrupted_run']=str(run[0]);STATE.write_text(json.dumps(state))
            rows=campaign.db.execute('SELECT event_date,currency,operation_type,operation_count,total_amount,min_amount,max_amount FROM analytics.daily_operation_summary ORDER BY 1,2,3').fetchall()
            Path('/reports/pre-kill-summary.json').write_text(json.dumps(rows,default=str))
        elif args.phase=='batch-recovered':
            from ledgerflow_ops.recovery import recover_runs
            state=json.loads(STATE.read_text());run=state['interrupted_run']
            # Age the QA metadata only: avoid a two-hour test wait. Financial data untouched.
            campaign.db.execute("UPDATE analytics.batch_runs SET started_at=clock_timestamp()-INTERVAL '3 hours' WHERE run_id=%s AND status='RUNNING'",(run,))
            campaign.check('Abandoned killed batch recovered under advisory lock',recover_runs(campaign.db)>=1)
            campaign.check('Interrupted run FAILED with AbandonedRun',campaign.db.execute('SELECT status,error_type FROM analytics.batch_runs WHERE run_id=%s',(run,)).fetchone()==('FAILED','AbandonedRun'))
            rows=campaign.db.execute('SELECT event_date,currency,operation_type,operation_count,total_amount,min_amount,max_amount FROM analytics.daily_operation_summary ORDER BY 1,2,3').fetchall()
            campaign.check('Published summary unchanged by abrupt batch kill',json.dumps(rows,default=str)==Path('/reports/pre-kill-summary.json').read_text())
            campaign.check('Interrupted staging and snapshot cleaned',campaign.scalar('SELECT count(*) FROM analytics.batch_source_snapshot WHERE run_id=%s',(run,))==0 and campaign.scalar('SELECT count(*) FROM analytics.daily_summary_stage WHERE run_id=%s',(run,))==0)
            campaign.args.skip_batch=False;campaign.batch()
        elif args.phase=='dlt':
            from confluent_kafka import Producer,Consumer
            from ledgerflow_ops.dead_letters import capture,replay,resolve_replays
            state=json.loads(STATE.read_text());event=state['event']
            original=dict(schemaVersion=1,eventId=event['id'],journalId=event['id'],operationType='DEPOSIT',debitAccountId=None,
                          creditAccountId=state['account'],amount='10.00',currency='EUR',occurredAt=event['createdAt'])
            producer=Producer({'bootstrap.servers':'kafka:19092','enable.idempotence':True})
            consumer=Consumer({'bootstrap.servers':'kafka:19092','group.id':'qa-incidents-'+uuid4().hex,'auto.offset.reset':'earliest','enable.auto.commit':False})
            topic='ledgerflow.operations.v1';consumer.subscribe([topic+'.DLT'])
            bad=json.dumps({**original,'schemaVersion':999}).encode()
            producer.produce(topic,key=event['id'].encode(),value=bad);campaign.check('Invalid Kafka message acknowledged',producer.flush(15)==0)
            deadline=time.monotonic()+60;message=None
            try:
                while time.monotonic()<deadline:
                    received=consumer.poll(.5)
                    if received and not received.error() and received.value()==bad: message=received;break
                campaign.check('Invalid schema delivered to real DLT',message is not None)
                capture(campaign.db,message)
                letter=campaign.db.execute('SELECT id FROM analytics.dead_letters WHERE topic=%s AND partition_id=%s AND offset_id=%s',(message.topic(),message.partition(),message.offset())).fetchone()[0]
                corrected=campaign.directory/'corrected.json';corrected.write_text(json.dumps(original))
                replay(campaign.db,producer,letter,topic,corrected)
                deadline=time.monotonic()+45
                while time.monotonic()<deadline:
                    resolve_replays(campaign.db)
                    if campaign.scalar('SELECT status FROM analytics.dead_letters WHERE id=%s',(letter,))=='RESOLVED':break
                    time.sleep(1)
                campaign.check('Corrected replay resolves without replacing original payload',campaign.db.execute('SELECT status,payload FROM analytics.dead_letters WHERE id=%s',(letter,)).fetchone()==('RESOLVED',bad))
                campaign.check('DLT replay cannot duplicate financial operation or fact',campaign.scalar('SELECT count(*) FROM analytics.transaction_facts WHERE event_id=%s',(event['id'],))==1 and campaign.scalar('SELECT balance FROM accounts WHERE id=%s',(state['account'],))==Decimal('15'))
            finally: consumer.close()
    except Exception as error:
        campaign.check('Incident probe completed without unexpected exception',False,type(error).__name__);raise
    finally:
        campaign.report();campaign.db.close()
    return int(any(c['status']=='FAIL' for c in campaign.checks))

if __name__=='__main__':raise SystemExit(main())
