
from parse import *
from database import DataBase
import redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

N = 10

db = DataBase(r)

r.execute_command('relcreate', 'student', 'id', 'name', 'int', 'varchar')
r.execute_command('relcreate', 'enroll', 'student_id', 'student_name', 'course_id', 'course_name', 'instructor_id', 'instructor_name', 'int', 'varchar', 'int', 'varchar', 'int', 'varchar')

r.execute_command('relinsert', 'student', '\r1223222', '\ramber')
r.execute_command('relinsert', 'student', '\r1238992', '\ramber')
r.execute_command('relinsert', 'student', '\r1234567', '\rjason')
r.execute_command('relinsert', 'student', '\r2015147587', '\rmjpyeon')
r.execute_command('relinsert', 'student', '\r2015147585', '\rymkim')
r.execute_command('relinsert', 'student', '\r2015147586', '\rhaksu')
r.execute_command('relinsert', 'student', '\r3015147586', '\rhaksu')
r.execute_command('relinsert', 'student', '\r44444', '\rhaksu')
r.execute_command('relinsert', 'student', '\r513513', '\ryonsei')
r.execute_command('relinsert', 'student', '\r1223922', '\rjason')
r.execute_command('relinsert', 'student', '\r1010101', '\rcarlson')
r.execute_command('relinsert', 'student', '\r4949494', '\rfredrick')
r.execute_command('relinsert', 'student', '\r3434343', '\rvincent')
r.execute_command('relinsert', 'student', '\r3423232', '\rvincent')
r.execute_command('relinsert', 'student', '\r1313131', '\rdean')
r.execute_command('relinsert', 'student', '\r3232323', '\rowen')
r.execute_command('relinsert', 'student', '\r1234567', '\rwilson')

r.execute_command('relinsert', 'enroll', '\r1223222', '\ramber', '\r10000', '\rCS50', '\r2015156662', '\rharvard prof1')
r.execute_command('relinsert', 'enroll', '\r1238992', '\ramber', '\r5000', '\rCS231n', '\r2014158492', '\rstanford prof1')
r.execute_command('relinsert', 'enroll', '\r1234567', '\rjason', '\r5000', '\rCS231n', '\r2014158493', '\rstanford prof2')
r.execute_command('relinsert', 'enroll', '\r2015147587', '\rmjpyeon', '\r10000', '\rCS50', '\r2015156663', '\rharvard prof2')
r.execute_command('relinsert', 'enroll', '\r2015147585', '\rymkim', '\r3000', '\r6.431', '\r4040317284', '\rmit prof1')
r.execute_command('relinsert', 'enroll', '\r2015147586', '\rhaksu', '\r4000', '\rCS224n', '\r3030101040', '\rstanford prof3')
r.execute_command('relinsert', 'enroll', '\r3015147586', '\rhaksu', '\r5000', '\rCS231n', '\r2014158492', '\rstanford prof1')
r.execute_command('relinsert', 'enroll', '\r44444', '\rhaksu', '\r10000', '\rCS50', '\r2015156662', '\rharvard prof1')
r.execute_command('relinsert', 'enroll', '\r513513', '\ryonsei', '\r3109', '\rCSI3109', '\r123456', '\ryonsei prof 1')
r.execute_command('relinsert', 'enroll', '\r1223922', '\rjason', '\r3109', '\rCSI3109', '\r123456', '\ryonsei prof 1')
r.execute_command('relinsert', 'enroll', '\r1010101', '\rcarlson', '\r3000', '\r6.431', '\r4040317284', '\rmit prof1')

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

