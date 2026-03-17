import sys
import os

# #region agent log
def _agent_log(location, message, data=None):
    try:
        import json, time, urllib.request
        payload = {
            "sessionId": "31fa06",
            "runId": "pre-fix",
            "hypothesisId": "ENTRY",
            "location": location,
            "message": message,
            "data": data or {},
            "timestamp": int(time.time() * 1000),
        }
        req = urllib.request.Request(
            "http://127.0.0.1:7580/ingest/8c20c5c3-d4fe-4238-8ce5-4d4b6328e630",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "X-Debug-Session-Id": "31fa06"},
            method="POST",
        )
        try:
            urllib.request.urlopen(req, timeout=0.25)
        except Exception:
            pass
    except Exception:
        pass

_agent_log("wsgi.py:import", "wsgi.imported", {"pid": os.getpid()})
# #endregion

# Add project folder to path
path = os.path.dirname(__file__)
if path not in sys.path:
    sys.path.insert(0, path)

from app import app as application  # noqa

# Initialise DB on first load
from database import init_db, migrate_db, seed_users
init_db()
migrate_db()
seed_users()
