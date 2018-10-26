from parse import Parser

# legal operations (tentative)
ops = ['show', 'create', 'insert', 'select']
# legal data types (tentative)
types = ['int', 'char']

class DataBase():
    def __init__(self, redis):
        self.parser = Parser()
        self.redis = redis
        self.table_nr = self.redis.zcard('table')

    # called upon new query from stdin
    def update_query(self, statement):
        self.parser = Parser(statement)

    def show(self):
        print("=================")
        print("Table list")
        print("=================")
        tables = [x.decode() for x in self.redis.zrange('table', 0, -1)]
        for table in tables:
            print(table)
        print("=================")

    def create(self):
        table = self.parser.get_table_name()
        if self.redis.zscore('table', table) is not None:
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

        self.redis.zadd('table', 0, table)
        self.redis.zincrby('table', table)
        self.table_nr = self.redis.zcard('table')

        self.show()
        return

    def insert(self):
        table = self.parser.get_table_name()
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
        
        assert len(type_list) == len(val_list)
        for i in range(len(type_list)):
            try:
                if type_list[i] == 'int':
                    eval(type_list[i])(val_list[i])
            except ValueError:
                # error message (tentative)
                print("value type mismatch")
                return
        
        field_list = self.redis.hkeys(table+':0')
        insert_dict = dict(zip(field_list, val_list))

        self.redis.hmset(table+':'+str(int(self.redis.zscore('table', table))), insert_dict)
        print("insert to hash " + table+':'+str(int(self.redis.zscore('table', table))))
        self.redis.zincrby('table', table)

        return

    # mjp
    def select(self):
        pass
        return

    def run_query(self):
        if self.parser.tokens[0] not in ops:
            # error message (tentative)
            print("SQL syntax error")
            return

        expression = "self." + self.parser.tokens[0] + "()"
        eval(expression)

