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
        print("before custom cmd exec")
        #print(self.redis.execute_command(*["relselect", "owie", "qweq"]))
        print(self.redis.execute_command('relcreate', 'student33', 'id', 'name', 'int', 'varchar'))
        print(self.redis.execute_command('relinsert', 'student33', 'id\r1232121', 'name\rjason'))
        #nodes = self.redis.connection_pool.nodes.all_nodes()
        #result = self.redis._execute_command_on_nodes(nodes, "relshow")
        result = self.redis.keys()
        print(result)
        '''
        print(type(result))
        print(type(result[0]))
        for x in range(len(result)):
            for y in range(len(result[x])):
                try:
                    print(result[x][y], end='')
                except Exception as e:
                    print(e)
                    pass
        '''
        #print([var.decode() for var in self.redis.execute_command(*["relselect", "owie", "qweq"])])
        print("after custom cmd exec")

    # called upon new query from stdin
    def update_query(self, statement):
        #self.parser = Parser(statement)
        self.statement = Parser(statement)

    # done
    def show(self):
        if self.parser.tokens.pop(0).lower() != 'tables':
            print("tables expected")
            return
        print("=================")
        print("Table list")
        print("=================")
        #tables = [x.decode() for x in self.redis.zrange('table', 0, -1)]
        [print(x.decode()) for x in self.redis.keys()]
        #for table in tables:
        #    print(table)
        print("=================")

    def create(self):
        table = self.parser.get_table_name()
        #if self.redis.zscore('table', table) is not None:
        if self.redis.sismember('table', table):
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
        columns = self.parser.get_column_names()
        print(columns)

        tables = self.parser.get_table_names()
        print(tables)
        
        return

    def delete(self):
        pass

    def update(self):
        pass

    def run_query(self):
        if self.parser.tokens[0].lower() not in ops:
            # error message (tentative)
            print("Unrecognized operation")
            return

        expression = "self." + self.parser.tokens.pop(0).lower() + "()"
        eval(expression)

