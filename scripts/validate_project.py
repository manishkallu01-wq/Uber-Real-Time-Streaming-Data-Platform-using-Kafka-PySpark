"""Dependency-free project contract gate."""
from pathlib import Path
import ast, sys
ROOT=Path(__file__).resolve().parents[1]
required=["README.md","src/event_contract.py","src/producer.py","spark/streaming_job.py","tests/test_event_contract.py","docker-compose.yml",".github/workflows/ci.yml"]
errors=[f"missing: {p}" for p in required if not (ROOT/p).is_file()]
for path in [ROOT/p for p in required if p.endswith(".py")]:
    try: ast.parse(path.read_text(),filename=str(path))
    except SyntaxError as exc: errors.append(str(exc))
readme=(ROOT/"README.md").read_text().lower()
for term in ["business","methodology","reproducibility","kafka","spark","results"]:
    if term not in readme: errors.append(f"README missing concept: {term}")
if errors: print("\n".join(f"ERROR {e}" for e in errors)); sys.exit(1)
print("PASS streaming project structure, syntax, and documentation contract")
