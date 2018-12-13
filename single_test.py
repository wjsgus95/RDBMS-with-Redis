
import redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

N = 10000

r.execute_command('relcreate', 'student', 'id', 'name', 'int', 'varchar')

for i in range(N):
    r.execute_command('relinsert', 'student', '\r1232121', '\rjason')
r.execute_command('relinsert', 'student', '\r1234567', '\rfound')

#result = r.execute_command('relselect', 'student', 'id\rname')
result = r.execute_command('relselect', 'student', ':*', '=id"1234567')
print(result)
