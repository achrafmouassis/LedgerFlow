"""Read-only smoke checks on main-app demo roles; never log tokens/passwords."""
import json
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request,urlopen

root=Path(__file__).resolve().parents[1]
credentials=json.loads((root/'.work/local-access.json').read_text())
results=[]
def request(path,token=None,body=None,key=None):
    headers={'Content-Type':'application/json'}
    if token: headers['Authorization']='Bearer '+token
    if key: headers['Idempotency-Key']=key
    try:
        with urlopen(Request('http://localhost:8080'+path,headers=headers,data=json.dumps(body).encode() if body is not None else None),timeout=30) as response:
            raw=response.read().decode();status=response.status
            try: content=json.loads(raw)
            except ValueError: content=raw
            return status,content,response.headers
    except HTTPError as error:return error.code,{},error.headers
def check(name,condition):
    results.append({'name':name,'status':'PASS' if condition else 'FAIL'})
    print(f'{results[-1]["status"]}: {name}')
    if not condition:raise AssertionError(name)
tokens={}
try:
    status,page,headers=request('/')
    check('Frontend serves real login HTML',status==200 and 'login-form' in page and 'workspace' in page)
    check('Content security policy present',"frame-ancestors 'none'" in headers.get('Content-Security-Policy',''))
    check('No browser session cookie',not headers.get('Set-Cookie'))
    check('JavaScript and stylesheet served',request('/app.js')[0]==request('/app.css')[0]==200)
    check('Anonymous bank request denied',request('/api/accounts')[0]==401)
    for name in ('admin','operator','analyst','client','client2'):
        status,result,_=request('/api/auth/login',body=credentials[name])
        check('Login '+name,status==200)
        tokens[name]=result['token']
    check('Admin can list users',request('/api/admin/users',tokens['admin'])[0]==200)
    check('Operator cannot administer',request('/api/admin/users',tokens['operator'])[0]==403)
    check('Analyst can inspect pipeline',request('/api/analytics/pipeline',tokens['analyst'])[0]==200)
    check('Analyst cannot create funds/accounts',request('/api/accounts',tokens['analyst'],dict(owner='FORBIDDEN',currency='EUR',openingBalance='0'))[0]==403)
    a=request('/api/accounts',tokens['client'])[1];b=request('/api/accounts',tokens['client2'])[1]
    check('Two clients have distinct assigned accounts',len(a)==len(b)==1 and a[0]['id']!=b[0]['id'])
    check('Client can read own account',request('/api/accounts/'+a[0]['id'],tokens['client'])[0]==200)
    check('Client cannot read another account',request('/api/accounts/'+b[0]['id'],tokens['client'])[0]==403)
    check('Client cannot inspect another journal',request('/api/accounts/'+b[0]['id']+'/journal',tokens['client'])[0]==403)
    check('Client cannot self-deposit',request('/api/accounts/'+a[0]['id']+'/deposits',tokens['client'],{'amount':'1'},'forbidden-client-cash')[0]==403)
    check('Client cannot read bank-wide analytics',request('/api/analytics/pipeline',tokens['client'])[0]==403)
    check('Overview exposes actual main dataset',request('/api/overview',tokens['operator'])[1]['operations']>=6120)
finally:
    for token in tokens.values(): request('/api/auth/logout',token,{})
    output=root/'.work/frontend-api-validation.json'
    output.write_text(json.dumps(results,indent=2),encoding='utf-8')
if any(r['status']=='FAIL' for r in results):raise SystemExit(1)
print(f'{len(results)} main-app smoke checks passed; no banking data written.')
