"""
gunicorn.conf.py — Myle Dashboard production config
"""
import os

workers      = 2
worker_class = "sync"
timeout      = 120
preload_app  = True
accesslog    = "-"
errorlog     = "-"
loglevel     = "info"

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
# #endregion


def on_starting(server):
    """Skip auto-start in master; workers will call start_scheduler() via post_fork."""
    os.environ['GUNICORN_MULTI_WORKER'] = '1'
    # #region agent log
    _agent_log("gunicorn.conf.py:on_starting", "gunicorn.on_starting", {"pid": os.getpid()})
    # #endregion


def post_fork(server, worker):
    """Start scheduler in exactly one worker (file lock prevents duplicates)."""
    # #region agent log
    _agent_log("gunicorn.conf.py:post_fork", "gunicorn.post_fork", {"pid": os.getpid()})
    # #endregion
    try:
        from app import start_scheduler
        start_scheduler()
    except Exception as exc:
        server.log.error(f"[Scheduler] post_fork start failed: {exc}")
