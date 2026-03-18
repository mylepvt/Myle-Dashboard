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


def on_starting(server):
    """Skip auto-start in master; workers will call start_scheduler() via post_fork."""
    os.environ['GUNICORN_MULTI_WORKER'] = '1'


def post_fork(server, worker):
    """Start scheduler in exactly one worker (file lock prevents duplicates)."""
    try:
        from app import start_scheduler
        start_scheduler()
    except Exception as exc:
        server.log.error(f"[Scheduler] post_fork start failed: {exc}")
