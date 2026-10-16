"""Validate native PBIP/PBIR files using Microsoft's public JSON schemas."""
import argparse
import hashlib
import json
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen
from jsonschema import validators
from referencing import Registry, Resource


def main():
    cli=argparse.ArgumentParser()
    cli.add_argument("--fetch-schemas",action="store_true",help="Download Microsoft schemas into .work (network required)")
    args=cli.parse_args()
    root=Path(__file__).resolve().parents[1]
    cache=root/".work/powerbi/schemas"
    cache.mkdir(parents=True,exist_ok=True)
    def load(uri):
        url=uri.split("#",1)[0]
        if urlparse(url).hostname != "developer.microsoft.com":
            raise ValueError("Only Microsoft report schemas are accepted")
        path=cache/(hashlib.sha256(url.encode()).hexdigest()+".json")
        if not path.exists():
            if not args.fetch_schemas:
                raise ValueError("Schema absent; run once with --fetch-schemas")
            with urlopen(url,timeout=30) as response:
                schema=json.load(response)
            path.write_text(json.dumps(schema),encoding="utf-8")
        return json.loads(path.read_text(encoding="utf-8"))
    registry=Registry(retrieve=lambda uri:Resource.from_contents(load(uri)))
    count=0
    for path in (root/"bi/powerbi").rglob("*"):
        if not path.is_file() or path.suffix not in (".json",".pbip",".pbir"):
            continue
        document=json.loads(path.read_text(encoding="utf-8"))
        if "$schema" not in document:
            continue
        uri=document["$schema"]
        schema=load(uri)
        scoped=registry.with_resource(uri,Resource.from_contents(schema))
        validator=validators.validator_for(schema)({"$ref":uri},registry=scoped)
        errors=list(validator.iter_errors(document))
        if errors:
            raise ValueError(f"{path.relative_to(root)}: {errors[0].message}")
        count+=1
    pages=root/"bi/powerbi/LedgerFlow.Report/definition/pages"
    visuals=0
    for path in pages.glob("*/page.json"):
        page=json.loads(path.read_text(encoding="utf-8"))
        for vpath in (path.parent/"visuals").glob("*/visual.json"):
            visual=json.loads(vpath.read_text(encoding="utf-8"))
            position=visual["position"]
            assert 0<=position["x"] and 0<=position["y"]
            assert position["x"]+position["width"]<=page["width"]
            assert position["y"]+position["height"]<=page["height"]
            visuals+=1
    print(f"{count} native Power BI files validated against Microsoft schemas; {visuals} visuals fit their pages")


if __name__=="__main__":
    main()
