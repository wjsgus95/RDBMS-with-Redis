import redis

N = 10000

r = redis.StrictRedis(host='localhost', port=6379, db=0)

def planA():
    r.flushall()
    r.hmset("student", {"id": "int", "name": "char"})
    for i in range(N):
        r.set(f'student:{i}:{0}', '1234567890')
        r.set(f'student:{i}:{1}', 'jason whatever')
    print(r.info('memory')['used_memory_human'])
    
def planB():
    r.flushall()
    r.hmset("student", {"id": "int", "name": "char"})
    for i in range(N):
        r.hmset(f'student:{i}', {"id": "1234567890", "name": "jason whatever"})
    print(r.info('memory')['used_memory_human'])

def planC():
    r.flushall()
    for i in range(N):
        r.lpush('mylist', '1234567890')
        r.lpush('mylist', 'jason whatever')
    print(r.info('memory')['used_memory_human'])

def planD():
    r.flushall()
    for i in range(N):
        r.lpush('mylist:0', '1234567890')
        r.lpush('mylist:1', 'jason whatever')
    print(r.info('memory')['used_memory_human'])


planA()
planB()
planC()
planD()
