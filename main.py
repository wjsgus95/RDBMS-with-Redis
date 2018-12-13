import redis
import sys
import signal

import subprocess

from rediscluster import StrictRedisCluster

from database import *
from parse import *

def sig_handler(sig, frame):
    print()
    subprocess.run([f"./{cluster_dir}/create-cluster", "stop"])
    subprocess.run([f"./{cluster_dir}/create-cluster", "clean"])
    print("\nGoodbye")
    sys.exit(0)

signal.signal(signal.SIGINT, sig_handler)
signal.signal(signal.SIGTSTP, sig_handler)

local = '127.0.0.1'
redis_dir = 'redis'
cluster_dir = f'{redis_dir}/utils/create-cluster'

cluster_spec = [{"host":local, "port":"6379"}, {"host":local, "port":"6380"},
                        {"host":local, "port":"6381"}]

subprocess.run([f"./{cluster_dir}/create-cluster", "stop"])
redis_server = subprocess.run([f"./{cluster_dir}/create-cluster", "start"])
redis_server = subprocess.run([f"./{cluster_dir}/create-cluster", "create"])

r = redis.StrictRedisCluster(startup_nodes=cluster_spec)
db = DataBase(r)

print('"Welcome DB with NoSQL"')

while True:
    statement = input("Program Query Input > ")

    db.update_query(statement)
    db.run_query()

