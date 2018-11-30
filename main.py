import redis
import sys
import signal

from database import *
from parse import Parser

def sig_handler(sig, frame):
    print("\nGoodbye")
    sys.exit(0)

signal.signal(signal.SIGINT, sig_handler)

r = redis.StrictRedis(host='localhost', port=6379, db=0)
db = DataBase(r)

print('"Welcome DB with NoSQL"')

while True:
    statement = input("Program Query Input > ")

    db.update_query(statement)
    db.run_query()
