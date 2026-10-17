"""Create ignored local credentials once. Never print passwords or tokens."""
import json
from pathlib import Path
import secrets

root = Path(__file__).resolve().parents[1]
path = root / '.env.auth'
if not path.exists():
    path.write_text('LEDGERFLOW_ADMIN_PASSWORD=' + secrets.token_urlsafe(24) + '\n', encoding='utf-8')
directory = root / '.work'
directory.mkdir(exist_ok=True)
access = directory / 'local-access.json'
if not access.exists():
    admin = next(line.split('=', 1)[1] for line in path.read_text().splitlines() if line.startswith('LEDGERFLOW_ADMIN_PASSWORD='))
    access.write_text(json.dumps({'admin': {'username': 'admin', 'password': admin},
        **{name: {'username': name, 'password': secrets.token_urlsafe(18)} for name in ('operator','analyst','client','client2')}}, indent=2), encoding='utf-8')
print('Local credentials ready in .work/local-access.json (ignored by Git).')
