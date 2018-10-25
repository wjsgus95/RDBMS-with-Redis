import shlex

class Parser():
    # new instance of Parser constructed everytime DataBase updates query
    def __init__(self, statement=''):
        if statement == '':
            return
        statement = statement.replace(';', '')
        statement = statement.replace('(', ' ')
        statement = statement.replace(')', ' ')
        statement = statement.replace(',', ' ')
        statement = statement.lower()
        self.tokens = shlex.split(statement)
        print(statement)

        self.key_val_dict = {}
        if self.tokens[0] == 'create':
            dict_start = self.tokens.index('table') + 2
            for i in range(dict_start, len(self.tokens), 2):
                self.key_val_dict[self.tokens[i]] = self.tokens[i+1]

        self.val_list = ()
        if self.tokens[0] == 'insert':
            dict_start = self.tokens.index('values') + 1
            for i in range(dict_start, len(self.tokens)):
                self.val_list.append(self.tokens[i])

    # for create operation
    def get_key_val_dict(self):
        return self.key_val_dict

    # for insert operation
    def get_val_list(self):
        return self.val_list

    def get_op(self):
        return self.tokens[0]

    # single table name to return in create, insert query
    def get_table_name(self):
        if self.get_op() == 'create':
            return self.tokens[self.tokens.index('table')+1]
        elif self.get_op() == 'insert':
            return self.tokens[self.tokens.index('into')+1]
        return None

    # multiple table names to return in select query
    def get_table_names(self):
        if self.get_op() == 'select':
            result = []
            for i in range(self.tokens.index('from')+1, len(self.tokens)):
                if self.tokens[i] == 'where':
                    break
                result.append(self.tokens[i])
            return result
        return []
