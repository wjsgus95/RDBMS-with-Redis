
import redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

N = 10

r.execute_command('relcreate', 'student', 'id', 'name', 'int', 'varchar')

r.execute_command('relinsert', 'student', '\r1234567', '\rjason')
r.execute_command('relinsert', 'student', '\r1010101', '\rcarlson')
r.execute_command('relinsert', 'student', '\r4949494', '\rfredrick')
r.execute_command('relinsert', 'student', '\r3434343', '\rvincent')
r.execute_command('relinsert', 'student', '\r1313131', '\rdean')
r.execute_command('relinsert', 'student', '\r3232323', '\rowen')
r.execute_command('relinsert', 'student', '\r1234567', '\rwilson')

#result = r.execute_command('relselect', 'student', 'id\rname')
result = r.execute_command('relselect', 'student', ':id\r:name', '|&<id"30000000\r=name"wilson\r&>id"4000000\r=name"fredrick')

print(result)
