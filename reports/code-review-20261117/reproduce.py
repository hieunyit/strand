#!/usr/bin/env python3
"""Run read-only review reproductions against the current reviewed working tree.
Expected: root/storage safety assertions fail; core observational test passes showing the gap.
No Docker or database fixtures are selected.
"""
import json, pathlib, shutil, subprocess, tempfile
here=pathlib.Path(__file__).resolve().parent
root=here.parent.parent
mapping=json.loads((here/"overlays.json").read_text())
replace={str(root/k):str(here/v) for k,v in mapping.items()}
go=shutil.which("go") or "/usr/local/go/bin/go"
with tempfile.TemporaryDirectory(prefix="strand-audit2-") as d:
    overlay=pathlib.Path(d)/"overlay.json"
    overlay.write_text(json.dumps({"Replace":replace}))
    packages=sorted({"./"+str(pathlib.Path(k).parent) for k in mapping})
    raise SystemExit(subprocess.call([go,"test","-p","1","-overlay",str(overlay),*packages,"-run","^(TestAudit2.*|TestAuditCompletionDropsBoundsGuard|TestAuditOldWorkerInsertedAfterTakeover|TestAuditPartitionedKeylessNeedsTableOID|TestAuditSpillExceedsTotalHardLimit)$","-count=1","-timeout=60s"],cwd=root))
