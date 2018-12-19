import shlex
import re
import functools
import operator

def remove_extra_blanks(statement):
    statement = statement.replace('\n', ' ').replace('\t', '').replace(';', '')
    statement = ' '.join(list(filter(lambda a: a != '', statement.split(' '))))
    return statement

def remove_outer_brackets(statement):
    if statement[0] == '(' and statement[-1] == ')' and statement.count('(') == 1 and statement.count(')') == 1:
        return statement[1:-1]
    else:
        return statement

def split_brackets(statement):
    statement = statement.replace('(', ' ( ')
    statement = statement.replace(')', ' ) ')
    return statement

def split_commas(statement):
    statement = statement.replace(',', ' , ')
    return statement

def parse_strings(statement):
    """
    replace ' ' in quotes with @
    """
    odd = True
    for i, c in enumerate(statement):
        if c =='\'' or c == '\"':
            if odd:
                start_idx = i
                odd = False
            else:
                end_idx = i
                statement = statement[:start_idx] + statement[start_idx:end_idx+1].replace(' ', '@') + statement[end_idx+1:]
                odd = True
    return statement

def split_operators(statement_list, operators):
    modified = []
    detected = []
    flag = False
    for i, s in enumerate(statement_list):
        if len(s) > 1:
            for op in operators:
                if op in s:
                    for d in detected:
                        if op in d:
                            flag = True
                    if flag:
                        flag = False
                        continue
                    if not isinstance(s, list):
                        statement_list[i] = []
                    temp =  s.split(op)
                    temp.insert(1, op)
                    statement_list[i] += temp
                    detected.append(op)
    #statement_list = functools.reduce(operator.iconcat, statement_list, [])
    for s in statement_list:
        if isinstance(s, list):
            modified += s
        else:
            modified.append(s)
    return modified


class Parser:
    def __init__(self, statement):
        self.statement = statement
        class_dict = {
            'create': CreateParser,
            'insert': InsertParser,
            'delete': DeleteParser,
            'update': UpdateParser,
            'select': SelectParser,
            'show': ShowParser}
        op = self.get_op()
        self.op_class = class_dict[op]

    def get_op(self):
        return self.statement.split(' ')[0].lower()

    def get_values(self):
        return self.op_class(self.statement).get_values()


class ShowParser:
    def __init__(self, statement):
        pass

    def get_values(self):
        return None



class CreateParser:
    def __init__(self, statement):
        statement = split_brackets(statement)
        statement = split_commas(statement)
        self.statement = remove_extra_blanks(statement)
        self.splitted = self.statement.split(' ')

    def get_values(self):
        s = self.splitted.index('(') + 1
        f = self.splitted.index(')')
        table = self.splitted[s-2]
        col_type = [v.lower() for i, v in enumerate(self.splitted[s:f]) if i % 3 == 1]
        col_name = [v for i, v in enumerate(self.splitted[s:f]) if i % 3 == 0]
        return {'table': table, 'col_type': col_type, 'col_name': col_name}

class InsertParser:
    def __init__(self, statement):
        statement = split_brackets(statement)
        statement = split_commas(statement)
        statement = parse_strings(statement)
        self.statement = remove_extra_blanks(statement)
        self.splitted = self.statement.split(' ')
        for i, t in enumerate(self.splitted):
            if t.lower() == 'into':
                self.i_start = i+1
                pass
            elif t.lower() == 'values':
                self.i_end = i
                self.v_start = i+1
                break

    def get_values(self):
        insert_into = self.splitted[self.i_start: self.i_end]
        values = self.splitted[self.v_start:]
        col_specified = False
        col_name = []
        col_val = []
        for i, t in enumerate(insert_into):
            # to do
            if i == 0:
                # table name
                table = t
            elif t == '(':
                # column specification detected
                col_specified = True
            elif t != ')' and t != ',':
                col_name.append(t)
            elif t == ')':
                break
        for i, t in enumerate(values):
            if t == ')':
                break
            elif t != '(' and t != ',':
                col_val.append(t)
        col_val = [s.replace('@', ' ').replace('\'', '').replace('\"', '') for s in col_val]
        if col_specified:
            col_val = [name+'\r'+val for name, val in zip(col_name, col_val)]
        else:
            col_val = ['\r' + val for val in col_val]
        return {'table': table, 'col_val': col_val}


class DeleteParser:
    def __init__(self, statement):
        statement = remove_extra_blanks(statement)
        self.statement = statement
        self.splitted = statement.split(' ')
        self.f_end = None
        for i, t in enumerate(self.splitted):
            if t.lower() == 'from':
                self.f_start = i+1
                pass
            elif t.lower() == 'where':
                self.f_end = i
                self.whereParser = WhereParser(' '.join(self.splitted[i+1:]))
                break

    def get_values(self):
        f = ' '.join(self.splitted[self.f_start:self.f_end]).replace(' ', '').replace(',', '\r').encode('ascii')
        try:
            w = self.whereParser.encode()
            return {'from': f, 'where': w}
        except:
            return {'from': f, 'where': ''}

class UpdateParser:
    def __init__(self, statement):
        statement = parse_strings(statement)
        statement = remove_extra_blanks(statement)
        self.statement = statement
        self.splitted = statement.split(' ')
        self.operators = ['=', '+', '-', '*']
        for i, t in enumerate(self.splitted):
            if t.lower() == 'update':
                self.s_start = i+1
            elif t.lower() == 'set':
                self.s_end = i
                self.f_start = i+1
                pass
            elif t.lower() == 'where':
                self.f_end = i
                self.whereParser = WhereParser(' '.join(self.splitted[i+1:]))
                break

    def get_values(self):
        s = ' '.join(self.splitted[self.s_start:self.s_end]).encode('ascii')
        f = ' '.join(self.splitted[self.f_start:self.f_end])
        f_list = f.split(' ')
        f_list = split_operators(f_list, self.operators)
        tokenized = []
        for i, val in enumerate(f_list):
            if i == 0:
                tokenized.append('')
            if val == ',':
                tokenized.append('')
                continue
            if val in self.operators and val != '=':
                tokenized[-1] = tokenized[-1].replace('=', '#')
            tokenized[-1] = tokenized[-1] + val
        f = '\r'.join(tokenized).replace('@',' ')
        f = f.replace('=', '^').replace(',', '\r').replace('\'', '').replace('\"', '').replace('^', '\"').encode('ascii')
        try:
            w = self.whereParser.encode()
            return {'update': s, 'set': f, 'where': w}
        except:
            return {'update': s, 'set': f, 'where': ''}



class SelectParser:
    def __init__(self, statement):
        self.select_keywords = ['count', 'sum']
        statement = parse_strings(statement)
        statement = remove_extra_blanks(statement)
        self.statement = statement
        self.splitted = statement.split(' ')

        self.s_start, self.s_end, self.f_start, self.f_end = 0,None,0,None
        self.w_start, self.w_end = 0, None
        self.g_idx = None
        self.h_idx = None

        # group by parsing under maintenance
        for i, t in enumerate(self.splitted):
            if t.lower() == 'select':
                self.s_start = i+1
            elif t.lower() == 'from':
                self.s_end = i
                self.f_start = i+1
                pass
            elif t.lower() == 'where':
                self.f_end = i
                self.w_start = i+1
                self.whereParser = WhereParser(' '.join(self.splitted[self.w_start:self.w_end]))
            # Note: where clause always precedes group by clause
            elif t.lower() == 'group' and self.splitted[i+1].lower() == 'by':
                self.w_end = i
                if self.f_end == None: self.f_end = i # if where clause not given
                if self.w_start == 0: self.w_start = self.w_end # if where clause not given
                self.whereParser = WhereParser(' '.join(self.splitted[self.w_start:self.w_end]))
                self.g_idx = i+2
            elif t.lower() == 'having':
                self.h_idx = i+1
                self.havingParser = WhereParser(' '.join(self.splitted[self.h_idx:]))
                break


    def parse_select(self, select):
        select_list = select.split('\r')
        founded_keyword = [None for t in select_list]
        for i, t in enumerate(select_list):
            for keyword in self.select_keywords:
                if keyword in t:
                    founded_keyword[i] = keyword
                    break
            if founded_keyword[i]:
                splitted = t.split('(')
                for s in splitted:
                    s = s.replace('(', '')
                select_list[i] = ':'.join([s for s in splitted if s!='']).replace('(','').replace(')','')
        for i, t in enumerate(select_list):
            if ':' not in t:
                select_list[i] = ':' + select_list[i]
        return '\r'.join(select_list).replace('count:', 'c:').replace('sum:', 's:')


    def get_values(self):
        s = ' '.join(self.splitted[self.s_start:self.s_end]).replace(' ', '').replace(',', '\r')
        s = self.parse_select(s)
        # deal with select *
        if '*' in s:
            s = ':*'
        s = s.encode('ascii')
        f = ' '.join(self.splitted[self.f_start:self.f_end]).replace(' ', '').replace(',', '\r').encode('ascii')
        g, h = '', ''
        try:
            w = self.whereParser.encode()
            if self.g_idx != None:
                g = self.splitted[self.g_idx].encode()
                if self.h_idx != None:
                    h = self.havingParser.encode()
            return {'select': s, 'from': f, 'where': w, 'group_by': g , 'having': h}
        except:
            if self.g_idx != None:
                g = self.splitted[self.g_idx].encode()
                if self.h_idx != None:
                    h = self.havingParser.encode()
            return {'select': s, 'from': f, 'where': '', 'group_by': g, 'having': h}

class WhereParser:
    def __init__(self, statement):
        self.conditional_operators = ['>=', '!=', '<=', '>', '=', '<', 'like']
        self.where_keywords = ['and', 'or', 'like']
        self.global_header = 0

        # two while loops to support having clause
        while statement.find('count(') >= 0:
            idx = statement.find('count(')
            statement = statement[0:idx] + statement[idx:idx+6].replace('count(', 'c:', 1) + statement[idx+6:].replace(')', '', 1)

        while statement.find('sum(') >= 0:
            idx = statement.find('sum(')
            statement = statement[0:idx] + statement[idx:idx+4].replace('sum(', 's:', 1) + statement[idx+6:].replace(')', '', 1)

        if statement != '':
            self.token_list = self.split_where_clause_to_list(statement)
        else:
            self.token_list = ''


    def uncapitalize_keywords(self, token_list):
        for i, t in enumerate(token_list):
            if t.lower() in self.where_keywords:
                token_list[i] = t.lower()
        return token_list



    def split_where_clause_to_list(self, statement):
        """
        ignore reduendantly doubly or more nested brackets
        """
        statement = split_brackets(statement)
        statement = parse_strings(statement)
        statement = remove_extra_blanks(statement)
        statement = remove_outer_brackets(statement)
        self.statement = statement
        statement_list = statement.split(' ')
        statement_list = split_operators(statement_list, self.conditional_operators[:-1])
        statement_list = [s for s in statement_list if s != '']
        statement_list = self.uncapitalize_keywords(statement_list)
        return statement_list

    def parse_condition(self):
        l = self.token_list[self.global_header]
        self.global_header += 1
        if l == '(':
            l = self.parse_cond_expr()
            self.global_header += 1
            return l
        elif self.global_header + 1 < len(self.token_list) and self.token_list[self.global_header] in self.conditional_operators:
            self.global_header += 2
            return (self.token_list[self.global_header-2], l, self.token_list[self.global_header-1])
        else:
            assert False,'Invalid Query: no matched condition in conditional statement or no matched bracket'

    def parse_and(self):
        l = self.parse_condition()
        while self.global_header < len(self.token_list) and self.token_list[self.global_header] == 'and':
            self.global_header += 1
            l = ('and', l, self.parse_condition())
        return l

    def parse_or(self):
        l = self.parse_and()
        while self.global_header < len(self.token_list) and self.token_list[self.global_header] == 'or':
            self.global_header += 1
            l = ('or', l, self.parse_and())
        return l

    def parse_cond_expr(self):
        encoded_tuple = self.parse_or()
        return encoded_tuple


    def inorder_tuple_traversal(self, parsed):
        if isinstance(parsed, tuple):
            encoded1 = self.inorder_tuple_traversal(parsed[0])
            encoded2 = self.inorder_tuple_traversal(parsed[1])
            encoded3 = self.inorder_tuple_traversal(parsed[2])
            if encoded1 in ['&', '|']:
                encoded = encoded1 + encoded2 + '\r' + encoded3
            elif encoded3[0] == '\'' or encoded3[0] == '\"' or encoded3[0].isdigit():
                encoded = encoded1 + encoded2 + '$' + encoded3
            else:
                encoded = encoded1 + encoded2 + '#' + encoded3
            return encoded
        elif isinstance(parsed, str):
            if parsed == 'and':
                parsed = '&'
            elif parsed == 'or':
                parsed = '|'
            elif parsed == 'like':
                parsed = '*'
            elif '@' in parsed:
                parsed = parsed.replace('@', ' ')
            return parsed
        else:
            raise TypeError
            exit()

    def parse(self):
        parsed = self.parse_cond_expr()
        self.global_header = 0
        return parsed

    def encode(self):
        if self.token_list == '':
            return ''
        encoded =  self.inorder_tuple_traversal(self.parse()).replace('\'', '').replace('\"', '').replace('$', '\"')

        return encoded.encode('ascii')

def parse_create(statement):
    parser = CreateParser(statement)
    return parser.get_values()

def parse_insert(statement):
    parser = InsertParser(statement)
    return parser.get_values()

def parse_select(statement):
    parser = SelectParser(statement)
    return parser.get_values()

def parse_delete(statement):
    parser = DeleteParser(statement)
    return parser.get_values()

def parse_update(statement):
    parser = UpdateParser(statement)
    return parser.get_values()

if __name__ == "__main__":
    create_query = 'CREATE table HA_HA_HO_HO (\n    haha   VARCHAR  ,\n  he_he  INT,hohoho   varchar\n)'
    insert_query1 = 'Insert INTO hello__haha \n VALUES \n (\"hey  hey\", 10, 25, \' hallo\')'
    insert_query2 = 'insert into KAkaKA \n (ha, he, hu, he) \n values \t (\"hey  hey\", 10, 25, \' hallo\')'
    delete_query = 'Delete  from student ,   teacher   WHERE student.id   > teacher.id and (( (  student.name like \"Myeon%Pyeo_\"  )) or   student.dept  =   \"C/S\")'
    update_query = 'UPDATE hello SET a =   \'10\'  , b= c - 3, CCAAS = \"haha  \" where (a > 10 or DeF_PP like \"5%a%b_C\") and (CCB=3);'
    select_query = 'select id,name,  count( ( dept))   , sum(adress )  From  student, teacher  WHERE (a > f   and b !=3 ) or (cc=\'and hello\' AND dd!=\'or\') and DF LIKE \'A%b\''
    create_parser = Parser(create_query)
    insert_parser1 = Parser(insert_query1)
    insert_parser2 = Parser(insert_query2)
    delete_parser = Parser(delete_query)
    update_parser = Parser(update_query)
    select_parser = Parser(select_query)
    create_parsed = create_parser.get_values()
    insert_parsed1 = insert_parser1.get_values()
    insert_parsed2 = insert_parser2.get_values()
    delete_parsed = delete_parser.get_values()
    update_parsed = update_parser.get_values()
    select_parsed = select_parser.get_values()
    print(parse_create(create_query))
    print(parse_insert(insert_query1))
    print(parse_insert(insert_query2))
    print(parse_delete(delete_query))
    print(parse_update(update_query))

    insert_query = 'insert into student values(20123123, "student1" );'
    print(parse_insert(insert_query))
    select_query = 'select name from student where (id <= 30000000 and name = "wilson") or (id > 40000000 and name = "fredrick") group by name having count(id) > 1000'
    print(parse_select(select_query))
    select_query = 'select name from student group by name having count(name) > 1'
    print(parse_select(select_query))
    #import ipdb; ipdb.set_trace()

    query = 'update student set id=id-5, name = "wilson2" where name like "h%"'
    print(parse_update(query))
