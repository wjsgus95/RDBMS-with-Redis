from parse import *

# legal operations
ops = ['show', 'create', 'insert', 'select', 'delete', 'update']
# legal data types
types = ['int', 'varchar']

class DataBase():
    def __init__(self, redis):
        self.redis = redis

    # called upon new query from stdin
    def update_query(self, statement):
        #self.parser = Parser(statement)
        self.statement = statement

    def show(self):
        print("=================")
        print("Table list")
        print("=================")
        [print(x.decode()) for x in self.redis.keys()]
        print("=================")

    '''
    query = dict(
        table: str(table name)
        col_name: list(str(column names))
        col_type: list(str(column data types))
        col_dict: dict(zip(col_name, col_type))
    )
    '''
    def create(self):
        query = parse_create(self.statement)

        #if self.redis.sismember('table', query['table']):
        #    # error message (tentative)
        #    print("table already exists")
        #    return

        #self.redis.hmset(f'{query["table"]}:0', query['col_dict')
        #self.redis.sadd('table', query['table'])

        #self.show()
        #return
        self.redis.execute_command('relcreate', query["table"], *query['col_name'], *query['col_type'])
        self.show()
        return


    '''
    query = dict(
        table: str(table name)
        col_val: list(str(column values))
    )
    '''
    def insert(self):
        query = parse_insert(self.statement)
        self.redis.execute_command('relinsert', query["table"], *query['col_val'])
        return


    '''
    query = dict(
        cols: list(str(column names))
        tables: list(str(table names))
        predicates: have to decide after implementing "select *"
    )
    '''
    def select(self):
        query = parse_select(self.statement)
        result = self.redis.execute_command('relselect', query["from"], query['select'], query['where'])
        print(result)
        return

    def delete(self):
        query = parse_delete(self.statement)

        return

    def update(self):
        query = parse_update(self.statement)

        return

    def run_query(self):
        if len(self.statement.split()) == 0: return
        elif self.statement.split()[0].lower() not in ops:
            # error message (tentative)
            print("Unrecognized operation")
            return

        expression = "self." + self.statement.split()[0].lower() + "()"
        eval(expression)

