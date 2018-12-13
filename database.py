from parse import Parser

# legal operations
ops = ['show', 'create', 'insert', 'select', 'delete', 'update']
# legal data types
types = ['int', 'varchar']

class DataBase():
    def __init__(self, redis):
        self.parser = Parser()
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
        qeury = parse_create(self.statement)

        #if self.redis.sismember('table', query['table']):
        #    # error message (tentative)
        #    print("table already exists")
        #    return

        #self.redis.hmset(f'{query["table"]}:0', query['col_dict')
        #self.redis.sadd('table', query['table'])

        #self.show()
        #return
        self.redis.execute_command('relcreate', f'query["table"]', *query['col_name'], *query['col_type'])
        self.show()
        return


    '''
    query = dict(
        table: str(table name)
        col_val: list(str(column values))
    )
    '''
    def insert(self):
        query = parse_insert(statement)

        
            
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
        
        return

    def delete(self):
        query = parse_delete(self.statement)

        return

    def update(self):
        query = parse_update(self.statement)

        return

    def run_query(self):
        if self.statement.split()[0].lower() not in ops:
            # error message (tentative)
            print("Unrecognized operation")
            return

        expression = "self." + self.statement.split()[0].lower() + "()"
        eval(expression)

