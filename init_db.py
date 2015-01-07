from api.database import init_db, init_engine
from api.app_config import DB_CONN
init_engine(DB_CONN)
init_db()
print "Done!"