
import redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

N = 10000

r.execute_command('relcreate', 'student', 'id', 'name', 'int', 'varchar')

for i in range(N):
    r.execute_command('relinsert', 'student', '\r1232121', '\rjason')

result = r.execute_command('relselect', 'id\rname', 'student')
