"""Dependency-free streaming project contract gate."""
from pathlib import Path
import ast, sys
ROOT=Path(__file__).resolve().parents[1]
required=["README.md","src/event_contract.py","src/producer.py","spark/streaming_job.py","tests/test_event_contract.py","docker-compose.yml","requirements.txt",".github/workflows/ci.yml"]
errors=[f"missing: {p}" for p in required if not (ROOT/p).is_file()]
for path in [ROOT/p for p in required if p.endswith(".py")]:
    try: ast.parse(path.read_text(),filename=str(path))
    except SyntaxError as exc: errors.append(str(exc))
readme=(ROOT/"README.md").read_text().lower()
for term in ["why it exists","implemented flow","event contract","results","processing guarantees","kafka","spark"]:
    if term not in readme: errors.append(f"README missing concept: {term}")
for unsupported in ["airflow is used","postgresql provides","nyc tlc trip data"]:
    if unsupported in readme: errors.append(f"README contains unsupported claim: {unsupported}")
if errors: print("\n".join(f"ERROR {e}" for e in errors)); sys.exit(1)
print("PASS streaming structure, syntax, and documentation contract")
