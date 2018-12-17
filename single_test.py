
from parse import *
from database import DataBase
import redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

N = 10

db = DataBase(r)

#r.execute_command('relcreate', 'student', 'id', 'name', 'int', 'varchar')

#r.execute_command('relinsert', 'student', '\r1223222', '\ramber')
#r.execute_command('relinsert', 'student', '\r1238992', '\ramber')
#r.execute_command('relinsert', 'student', '\r1234567', '\rjason')
#r.execute_command('relinsert', 'student', '\r1223922', '\rjason')
#r.execute_command('relinsert', 'student', '\r1010101', '\rcarlson')
#r.execute_command('relinsert', 'student', '\r4949494', '\rfredrick')
#r.execute_command('relinsert', 'student', '\r3434343', '\rvincent')
#r.execute_command('relinsert', 'student', '\r3423232', '\rvincent')
#r.execute_command('relinsert', 'student', '\r1313131', '\rdean')
#r.execute_command('relinsert', 'student', '\r3232323', '\rowen')
#r.execute_command('relinsert', 'student', '\r1234567', '\rwilson')

#result = r.execute_command('relselect', 'student', 'id\rname')
#print(result)
#result = r.execute_command('relselect', 'student', 's:id\r:name', '', 'name')
#print(result)
#result = r.execute_command('relselect', 'student', 'c:id\r:name', '', 'name')
#print(result)
#result = r.execute_command('relselect', 'student', ':id\rc:name', '', 'name')
#print(result)
#result = r.execute_command('relselect', 'student', ':*', '', 'name')
#print(result)
#result = r.execute_command('reldelete', 'student', '=name"vincent')
#print(result)
#result = r.execute_command('relselect', 'student', ':name', '', 'name', '>c:name"1')
#print(result)
##result = r.execute_command('relupdate', 'student', 'id#id+3\rname"mjpyeon', '=name"amber')
##print(result)
#result = r.execute_command('relselect', 'student', ':*', '', '', '')
#print(result)

#select_query = 'select id, name from student where (id < 3000) or (id > 3000)'
#select_query = parse_select(select_query)
#print(select_query)

while True:
    try:
        statement = input("Program Query Input > ")
    except EOFError:
        exit()

    print(statement)
    db.update_query(statement)
    db.run_query()

