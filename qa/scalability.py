"""Open-arrival load against isolated QA, never main. Bounded outstanding work.
Measures intended-arrival latency as well as HTTP latency: slower emission is
not silently treated as lower offered load. Reports dropped arrivals as failure.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
import json
import time
import uuid
from urllib.request import Request, urlopen
from run import Campaign, BASE, percentile


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--replicas',type=int,required=True,choices=[1,2,3])
    parser.add_argument('--seconds',type=int,default=60)
    parser.add_argument('--rate',type=int,default=40)
    args=parser.parse_args()
    if not 10<=args.seconds<=3600 or not 1<=args.rate<=500:
        parser.error('seconds 10..3600; rate 1..500')
    campaign=Campaign(argparse.Namespace(phase=f'scale-{args.replicas}',skip_batch=True,p95_ms=2000,drain_seconds=600))
    campaign.load_model='open-arrival scheduling with bounded outstanding requests; generator drops are failures'
    try:
        # Verify a single session is accepted by every replica through the gateway.
        instances=set()
        for _ in range(30):
            with urlopen(Request(BASE+'/overview',headers={'Authorization':'Bearer '+campaign.token}),timeout=20) as response:
                instances.add(json.load(response)['instance'])
        campaign.check('Gateway distributes one authenticated session across expected instances',len(instances)==args.replicas,sorted(instances))
        accounts=[campaign.account(('MAD','EUR','USD')[i%3], '10000') for i in range(48)]
        campaign.group=f'open-arrival-{args.replicas}'
        total=args.seconds*args.rate
        pending,completed,dropped=[],[],0
        start=time.perf_counter()+.5
        def work(index,scheduled):
            response=campaign.cash(accounts[index%48], '1.25', key=f'scale-{campaign.run_id}-{index}')
            response['arrival_ms']=(time.perf_counter()-scheduled)*1000
            return index,response
        with ThreadPoolExecutor(64) as pool:
            for index in range(total):
                scheduled=start+index/args.rate
                delay=scheduled-time.perf_counter()
                if delay>0: time.sleep(delay)
                active=[]
                for future in pending:
                    if future.done(): completed.append(future.result())
                    else: active.append(future)
                pending=active
                if len(pending)>=128: dropped+=1
                else: pending.append(pool.submit(work,index,scheduled))
            completed.extend(f.result() for f in pending)
        elapsed=time.perf_counter()-start
        for index,response in completed:
            if response['status']==200: campaign.expected[accounts[index%48]]+=Decimal('1.25')
        errors=sum(r['status']!=200 for _,r in completed)
        result=dict(replicas=args.replicas,seconds=args.seconds,offered_rps=args.rate,offered_requests=total,
                    completed=len(completed),dropped_arrivals=dropped,unexpected_errors=errors,
                    achieved_rps=len(completed)/elapsed,p95_http_ms=percentile([r['elapsed_ms'] for _,r in completed],.95),
                    p95_arrival_ms=percentile([r['arrival_ms'] for _,r in completed],.95),instances=sorted(instances),
                    note='Same local Docker host; one CPU and 768 MiB per backend; shared PostgreSQL and one Kafka broker.')
        (campaign.directory/'scalability.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
        campaign.stages.append(dict(concurrency=64,requests=len(completed),errors=errors,seconds=elapsed,
            requests_per_second=result['achieved_rps'],p50_ms=percentile([r['arrival_ms'] for _,r in completed],.5),
            p95_ms=result['p95_arrival_ms'],p99_ms=percentile([r['arrival_ms'] for _,r in completed],.99)))
        campaign.check('No dropped arrivals',dropped==0,result)
        campaign.check('No unexpected HTTP error',errors==0,result)
        campaign.check('Arrival-to-response p95 under 2s',result['p95_arrival_ms']<=2000,result)
        campaign.reconcile()
        print(json.dumps(result),flush=True)
    except Exception as error:
        campaign.check('Load completed without exception',False,type(error).__name__)
        raise
    finally:
        campaign.report();campaign.db.close()
    return int(any(c['status']=='FAIL' for c in campaign.checks))

if __name__=='__main__': raise SystemExit(main())
