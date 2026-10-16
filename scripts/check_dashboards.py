"""Check provisioned panels against Grafana's real SQL API (local development)."""
import base64
import json
import os
from pathlib import Path
from urllib.request import Request, urlopen


def main():
    root=Path(__file__).resolve().parents[1]
    credentials="admin:"+os.environ.get("GRAFANA_ADMIN_PASSWORD","ledgerflow-local-admin")
    headers={"Authorization":"Basic "+base64.b64encode(credentials.encode()).decode(),"Content-Type":"application/json"}
    checked=0
    for path in (root/"infrastructure/grafana/dashboards").glob("*.json"):
        dashboard=json.loads(path.read_text(encoding="utf-8"))
        for panel in dashboard["panels"]:
            queries=[]
            for target in panel["targets"]:
                target={**target,"rawSql":target["rawSql"].replace("${currency:sqlstring}","'EUR'"),
                    "intervalMs":30000,"maxDataPoints":1000}
                queries.append(target)
            request=Request("http://localhost:3000/api/ds/query",headers=headers,
                data=json.dumps({"queries":queries,"from":"now-7d","to":"now"}).encode())
            with urlopen(request,timeout=20) as response:
                result=json.load(response)
            for entry in result["results"].values():
                if entry.get("error") or entry.get("status",200)>=400:
                    raise RuntimeError(f"Panel failed: {panel['title']}: {entry.get('error','query error')}")
            checked+=1
    print(f"{checked} dashboard panels queried successfully through Grafana")


if __name__=="__main__":
    main()
