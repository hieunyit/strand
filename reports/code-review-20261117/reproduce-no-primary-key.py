#!/usr/bin/env python3
"""Run no-PK audit overlays; NK01 safety assertions intentionally fail until fixed."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

report = Path(__file__).resolve().parent
root = report.parent.parent
go = shutil.which("go") or "/usr/local/go/bin/go"
cases = [
    ("citext-gate_test.go.txt", "internal/pipeline/nokey_pipeline_audit_test.go", "./internal/pipeline", "^TestAuditKeylessCitextGate$"),
    ("reader-order_test.go.txt", "internal/reader/pg/nokey_audit_test.go", "./internal/reader/pg", "^TestAuditIndexOrder$"),
    ("target-subset_test.go.txt", "internal/validate/nokey_audit_test.go", "./internal/validate", "^TestAuditSubset$"),
    ("writer-null_test.go.txt", "internal/writer/nokey_writer_audit_test.go", "./internal/writer", "^TestAuditKeyless(NullLiteral|ChangedControl)"),
]
failed = False
with tempfile.TemporaryDirectory(prefix="strand-no-pk-") as tmp:
    for i, (source, target, package, pattern) in enumerate(cases):
        overlay = Path(tmp) / (str(i) + ".json")
        overlay.write_text(json.dumps({"Replace": {str(root / target): str(report / "evidence/no-primary-key" / source)}}))
        print("\n" + source, flush=True)
        result = subprocess.run([go, "test", "-overlay", str(overlay), package, "-run", pattern, "-count=1", "-v"], cwd=root)
        failed |= result.returncode != 0
raise SystemExit(1 if failed else 0)
