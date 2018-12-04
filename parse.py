import shlex

class Parser():
    # new instance of Parser constructed everytime DataBase updates query
    def __init__(self, statement=''):
        if statement == '':
            return
        statement = statement.replace(';', '')
        statement = statement.replace('(', '( ')
        statement = statement.replace(')', ' )')
        statement = statement.replace(',', ' , ')
        self.tokens = shlex.split(statement)

        self.op = self.tokens[0].lower()

        self.field_type_dict = {}
        if self.tokens[0].lower() == 'create':
            #dict_start = self.tokens.index('table') + 2
            #for i in range(dict_start, len(self.tokens), 2):
            #    self.field_type_dict[self.tokens[i]] = self.tokens[i+1]
            dict_start = self.tokens.index('table') + 3
            assert self.tokens[self.tokens.index('table')+2] == '('
            for i in range(dict_start, len(self.tokens), 3):
                self.field_type_dict[self.tokens[i]] = self.tokens[i+1]
                assert self.tokens[i+2] == ','

        self.val_list = []
        if self.tokens[0].lower() == 'insert':
            #dict_start = self.tokens.index('values') + 1
            dict_start = self.tokens.index('values') + 2
            for i in range(dict_start, len(self.tokens)-1):
                self.val_list.append(self.tokens[i])

    # for create operation
    def get_field_type_dict(self):
        return self.field_type_dict

    # for insert operation
    def get_val_list(self):
        return self.val_list

    def get_op(self):
        return self.op

    # single table name to return in create, insert query
    def get_table_name(self):
        if self.get_op() == 'create':
            return self.tokens[self.tokens.index('table')+1]
        elif self.get_op() == 'insert':
            return self.tokens[self.tokens.index('into')+1]
        return None

    # multiple column names to return in select query
    def get_column_names(self):
        if self.get_op() == 'select':
            result = []
            while len(self.tokens) > 0 and self.tokens[0].lower() != 'from':
                result.append(self.tokens.pop(0))
            self.tokens.pop(0) # consume "from" token 
            return result
        return None

    # multiple table names to return in select query
    def get_table_names(self):
        if self.get_op() == 'select':
            result = []
            while len(self.tokens) > 0 and self.tokens[0].lower() != 'where' and self.tokens[0] != ',':
                result.append(self.tokens.pop(0))
            return result
        return None

    def get_predicates(self):
        if self.tokens[0].lower() != 'where':
            return None
        else:
            self.tokens.pop(0)
            return predicate
