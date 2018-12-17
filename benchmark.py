import redis
import subprocess
from time import *
from rediscluster import StrictRedisCluster

local = '127.0.0.1'
redis_dir = 'redis'
cluster_dir = f'{redis_dir}/utils/create-cluster'

cluster_spec = [{"host":local, "port":"6379"}, {"host":local, "port":"6380"}, {"host":local, "port":"6381"}]

subprocess.run([f"./{cluster_dir}/create-cluster", "stop"])
redis_server = subprocess.run([f"./{cluster_dir}/create-cluster", "start"])
redis_server = subprocess.run([f"./{cluster_dir}/create-cluster", "create"])

r = redis.StrictRedisCluster(startup_nodes=cluster_spec)

N = 10000
print(f'N = {N}\n')

def benchmark_callback(plan):
    r.flushall()
    start = time()
    plan()
    print(f'{str(plan).split()[1]} : {round(time()-start, 2)}s')
    print('node 127.0.0.1:6379', r.info('memory')['127.0.0.1:6379']['used_memory_human'])
    print('node 127.0.0.1:6380', r.info('memory')['127.0.0.1:6380']['used_memory_human'])
    print('node 127.0.0.1:6381', r.info('memory')['127.0.0.1:6380']['used_memory_human'])
    print()

def initial_memory_usage():
    return

def linked_list_insertion():
    for i in range(N):
        r.lpush('student', '1234567890')
        r.lpush('student', 'jason whatever')
        r.lpush('student', 'computer science')

#def linked_list_per_col_insertion():
#    r.hmset("student", {"id": "int", "name": "char"})
#    for i in range(N):
#        r.lpush('student:0', '1234567890')
#        r.lpush('student:1', 'jason whatever')
#        r.lpush('student:2', 'computer science')
#
def our_approach():
    r.execute_command('relcreate', 'student', 'id', 'name', 'int', 'varchar')
    for i in range(N):
        r.execute_command('relinsert', 'student', '\r1232121', '\rjason')
    #print(r.execute_command('relselect', 'student', ':id\r:name'))

def set_insertion():
    r.hmset("student", {"id": "int", "name": "char"})
    for i in range(N):
        r.set(f'student:{i}:{0}', '1234567890')
        r.set(f'student:{i}:{1}', 'jason whatever')
        r.set(f'student:{i}:{2}', 'computer science')
    
def hashset_insertion():
    r.hmset("student", {"id": "int", "name": "char", 'dept':'char'})
    for i in range(N):
        r.hmset(f'student:{i}', {"id": "1234567890", "name": "jason whatever", 'dept': 'computer science'})

#def sadd_insertion():
#    r.hmset("student", {"id": "int", "name": "char"})
#    for i in range(N):
#        r.sadd(f'student:{i}', '1234567890\0jason whatever\0computer science')

# put locally defined functions in a list
benchmarks = [function[0] for function in locals().items() if 'function' in str(function[1]) and 'at' in str(function[1])]
benchmarks = benchmarks[1:] # remove callback function itself
benchmarks = [eval(elt) for elt in benchmarks]

for benchmark in benchmarks:
    benchmark_callback(benchmark)
    
subprocess.run([f"./{cluster_dir}/create-cluster", "stop"])
subprocess.run([f"./{cluster_dir}/create-cluster", "clean"])
