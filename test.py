import redis
from time import *

N = 100000
print(f'N = {N}\n')

r = redis.StrictRedis(host='localhost', port=6379, db=0)

def benchmark_callback(plan):
    r.flushall()
    start = time()
    plan()
    print(f'{str(plan).split()[1]} : {round(time()-start, 2)}s', end =', ')
    print(r.info('memory')['used_memory_human'])

def set_insertion():
    r.hmset("student", {"id": "int", "name": "char"})
    for i in range(N):
        r.set(f'student:{i}:{0}', '1234567890')
        r.set(f'student:{i}:{1}', 'jason whatever')
    
def hashset_insertion():
    r.hmset("student", {"id": "int", "name": "char"})
    for i in range(N):
        r.hmset(f'student:{i}', {"id": "1234567890", "name": "jason whatever"})

def linked_list_insertion():
    for i in range(N):
        r.lpush('student', '1234567890')
        r.lpush('student', 'jason whatever')

# most tempting
def linked_list_per_col_insertion():
    r.hmset("student", {"id": "int", "name": "char"})
    for i in range(N):
        r.lpush('student:0', '1234567890')
        r.lpush('student:1', 'jason whatever')


# put locally defined functions in a list
benchmarks = [function[0] for function in locals().items() if 'function' in str(function[1]) and 'at' in str(function[1])]
benchmarks = benchmarks[1:]
benchmarks = [eval(elt) for elt in benchmarks]

for benchmark in benchmarks:
    benchmark_callback(benchmark)
