import sys
import os

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
