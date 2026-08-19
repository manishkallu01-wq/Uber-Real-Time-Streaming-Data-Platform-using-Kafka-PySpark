"""Versioned trip-event contract with dependency-free validation."""
from __future__ import annotations
from datetime import datetime
REQUIRED={"schema_version","event_id","trip_id","event_time","city","fare_usd","distance_km"}

def validate_event(event: dict) -> list[str]:
    errors=[f"missing {key}" for key in sorted(REQUIRED-event.keys())]
    if errors: return errors
    if event["schema_version"] != 1: errors.append("unsupported schema_version")
    for key in ("event_id","trip_id","city"):
        if not isinstance(event[key],str) or not event[key].strip(): errors.append(f"{key} must be non-empty")
    try: datetime.fromisoformat(str(event["event_time"]).replace("Z","+00:00"))
    except ValueError: errors.append("event_time must be ISO-8601")
    for key in ("fare_usd","distance_km"):
        if not isinstance(event[key],(int,float)) or event[key] < 0: errors.append(f"{key} must be non-negative")
    return errors
