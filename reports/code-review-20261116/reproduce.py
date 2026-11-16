#!/usr/bin/env python3
"""Reproduce failures on the reviewed baseline without modifying the working tree.
Expected exit code: 1 (regressions fail on the original code).
"""
import io, json, os, pathlib, shutil, subprocess, tarfile, tempfile
here = pathlib.Path(__file__).resolve().parent
repo = here.parent.parent
baseline = "f830bfd8aa3d6f9118cd79aebce69245086700cc"
paths = {"internal_web": "internal/web", "internal_reader_pg": "internal/reader/pg", "internal_typemap": "internal/typemap", "internal_snapshot": "internal/snapshot"}
with tempfile.TemporaryDirectory(prefix="strand-audit-baseline-") as temp:
    root = pathlib.Path(temp) / "source"
    root.mkdir()
    data = subprocess.check_output(["git", "archive", baseline], cwd=repo)
    with tarfile.open(fileobj=io.BytesIO(data)) as archive:
        archive.extractall(root, filter="data")
    mapping = {}
    packages = set()
    for evidence in here.glob("internal_*_audit_review_test.go.txt"):
        stem = evidence.name.removesuffix("_audit_review_test.go.txt")
        pkg = paths[stem]
        mapping[str(root / pkg / "audit_review_test.go")] = str(evidence)
        packages.add("./" + pkg)
    overlay = pathlib.Path(temp) / "overlay.json"
    overlay.write_text(json.dumps({"Replace": mapping}))
    go = shutil.which("go") or "/usr/local/go/bin/go"
    env = os.environ.copy()
    env["PATH"] = str(pathlib.Path(go).parent) + os.pathsep + env.get("PATH", "")
    raise SystemExit(subprocess.call([go, "test", "-overlay", str(overlay), *sorted(packages), "-run", "^TestAudit", "-count=1", "-timeout=60s"], cwd=root, env=env))
