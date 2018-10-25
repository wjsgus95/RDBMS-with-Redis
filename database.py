from parse import Parser

# legal operations (tentative)
ops = ['show', 'create', 'insert', 'select']

class DataBase():
    def __init__(self, redis):
        self.parser = Parser()
        self.table_nr = 0
        self.redis = redis

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
        if self.redis.zadd('table', self.table_nr, self.parser.get_table_name()) == 0:
            # tentative error message
            print("table already exists")
            return
        self.table_nr += 1

        key_val_dict = self.parser.get_key_val_dict()
        for key in key_val_dict:
            self.redis.hset(self.parser.get_table_name()+':0', key, key_val_dict[key])
        self.show()
        return

    def insert(self):
        self.redis.hgetall(self.parser.get_table_name()+':0')
        val_list = self.parser.get_val_list()
        pass
        return

    def select(self):
        pass
        return

    def run_query(self):
        assert self.parser.tokens[0] in ops
        expression = "self." + self.parser.tokens[0] + "()"
        eval(expression)

