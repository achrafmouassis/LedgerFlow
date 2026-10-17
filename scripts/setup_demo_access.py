"""Provision local demo roles through authenticated admin API; no plaintext logs."""
import json
from pathlib import Path
from urllib.request import Request, urlopen
root=Path(__file__).resolve().parents[1]
credentials=json.loads((root/'.work/local-access.json').read_text())
base='http://localhost:8080/api'
def call(path,method='GET',body=None,token=None):
    headers={'Content-Type':'application/json'}
    if token: headers['Authorization']='Bearer '+token
    with urlopen(Request(base+path,data=json.dumps(body).encode() if body is not None else None,headers=headers,method=method),timeout=30) as response:
        return json.load(response)
token=call('/auth/login','POST',credentials['admin'])['token']
existing={u['username']:u for u in call('/admin/users',token=token)}
for name,role in [('operator','OPERATOR'),('analyst','VIEWER'),('client','CUSTOMER'),('client2','CUSTOMER')]:
    if name not in existing:
        existing[name]=call('/admin/users','POST',{**credentials[name],'role':role},token)
accounts=call('/accounts?size=100',token=token)
for name,account in zip(('client','client2'),accounts[:2]):
    call(f'/admin/users/{existing[name]["id"]}/accounts/{account["id"]}','PUT',token=token)
call('/auth/logout','POST',token=token)
print('Demo roles provisioned; two clients have distinct explicitly assigned accounts. Credentials remain local.')
