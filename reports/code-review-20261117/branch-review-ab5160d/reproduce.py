#!/usr/bin/env python3
"""Read-only branch-review repros. Safety assertions intentionally fail on reviewed HEAD."""
import json
import shutil
import subprocess
import tempfile
from pathlib import Path
r=Path(__file__).resolve().parent
root=r.parent.parent.parent
go=shutil.which("go") or "/usr/local/go/bin/go"
results=[]
with tempfile.TemporaryDirectory(prefix="strand-branch-review-") as tmp:
 for case in json.loads((r/"repro-cases.json").read_text()):
  overlay=Path(tmp)/(case["name"]+".json")
  overlay.write_text(json.dumps({"Replace":{str(root/m["virtual"]):str(r/m["source"]) for m in case["mappings"]}}))
  cmd=[go,"test","-overlay",str(overlay),case["package"],"-run",case["pattern"],"-count=1","-v"]
  with (r/"evidence"/(case["name"]+"-repro.log")).open("w") as out:
   p=subprocess.run(cmd,cwd=root,stdout=out,stderr=subprocess.STDOUT)
  results.append({"name":case["name"],"exit_code":p.returncode,"expected_on_reviewed_head":case["observed_exit"],"command":cmd[:2]+["-overlay","<temporary mapping>"]+cmd[4:]})
  print(case["name"],"exit",p.returncode,flush=True)
(r/"evidence/repro-results.json").write_text(json.dumps(results,indent=2))
raise SystemExit(1 if any(x["exit_code"] for x in results) else 0)
