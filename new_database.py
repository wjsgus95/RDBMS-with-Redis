from parse import Parser

# legal operations
ops = ['show', 'create', 'insert', 'select', 'delete', 'update']
# legal data types
types = ['int', 'varchar']

class DataBase():
    def __init__(self, redis):
        self.parser = Parser()
        self.redis = redis
        #self.table_nr = self.redis.zcard('table')

    # called upon new query from stdin
    def update_query(self, statement):
        #self.parser = Parser(statement)
        self.statement = statement

    def show(self):
        print("=================")
        print("Table list")
        print("=================")
        [print(x.decode()) for x in self.redis.smembers('table')]
        print("=================")

    '''
    query :=  dict(
        table: str(table name)
        col_name: list(str(column names))
        col_type: list(str(column data types))
        col_dict: dict(zip(col_name, col_type))
    )
    '''
    def create(self):
        qeury = parse_create(self.statement)

        if self.redis.sismember('table', query['table']):
            # error message (tentative)
            print("table already exists")
            return

        field_type_dict = self.parser.get_field_type_dict()
        assert len(field_type_dict) > 0
        print(field_type_dict.values())
        if not all(type in types for type in field_type_dict.values()):
            # error message (tentative)
            print("invalid data type")
            return

        self.redis.hmset(table+':0', field_type_dict)

        #self.redis.zadd('table', 0, table)
        #self.redis.zincrby('table', table)
        self.redis.sadd('table', table)
        #self.table_nr = self.redis.zcard('table')

        self.show()
        return

    def insert(self):
        query = parse_insert(statement)

        #table = self.parser.get_table_name()
        type_list = self.redis.hvals(table+':0')
        if len(type_list) == 0:
            # error message (tentative)
            print("table does not exist")
            return

        val_list = self.parser.get_val_list()

        if len(type_list) != len(val_list):
            # error message (tentative)
            print("number of values mismatch")
            return
        
        # number of input checking
        assert len(type_list) == len(val_list)
        # type checking
        for i in range(len(type_list)):
            try:
                if type_list[i] == 'int':
                    if type_list[i].lower() == 'null':
                        type_list[i] = type_list[i].lower()
                        continue
                    eval(type_list[i])(val_list[i])
            except ValueError:
                # error message (tentative)
                print("value type mismatch")
                return
        
        field_list = self.redis.hkeys(table+':0')
        insert_dict = dict(zip(field_list, val_list))

        #self.redis.hmset(table+':'+str(int(self.redis.zscore('table', table))), insert_dict)
        for column in insert_dict:
            # enclose table name with braces so all the columns fall into same slot in cluster
            self.redis.lpush(f'{{table}}:{column}', insert_dict[column])
        #print("insert to hash " + table+':'+str(int(self.redis.zscore('table', table))))
        #self.redis.zincrby('table', table)

        return

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

