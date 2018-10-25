import redis
import sys

from database import *
from parse import Parser

r = redis.StrictRedis(host='localhost', port=6379, db=0)
db = DataBase(r)

print('"Welcome DB with NoSQL"')

while True:
    statement = input("Program Query Input > ")

    db.update_query(statement)
    db.run_query()
