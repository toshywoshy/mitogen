
import sqlite3

import mitogen.dbapi
import mitogen.master

import logging
logging.basicConfig(level=logging.DEBUG)

router = mitogen.master.Router()
local = router.local()

conn = mitogen.dbapi.connect(local, sqlite3, ':memory:')
print conn

curs = conn.cursor()
print curs
