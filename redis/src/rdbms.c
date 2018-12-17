#include "server.h"

#define need_expand(x) ((((x)->length >= (x)->max_length)))
#define group_need_expand(x) ((((x)->group_length >= (x)->group_max_length)))

int having_op(char*, long long, int);
int get_having_literal(char*);

// not using this for now, incompatible with redis-cluster-py, use KEYS command instead
void relshowCommand(client* c) {
    dictIterator *di;
    dictEntry *de;
    int allkeys = 1;
    unsigned long numkeys = 0;
    void *replylen = addDeferredMultiBulkLength(c);

    di = dictGetSafeIterator(c->db->dict);

    while((de = dictNext(di)) != NULL) {
        sds key = dictGetKey(de);
        robj *keyobj;

        if (allkeys) {
            keyobj = createStringObject(key,sdslen(key));
            if (!keyIsExpired(c->db,keyobj)) {
                addReplyBulk(c,keyobj);
                numkeys++;
            }
            decrRefCount(keyobj);
        }
    }
 
    setDeferredMultiBulkLength(c,replylen,numkeys);
}

void relinsertCommand(client* c) {
    // presumably call by reference
    robj* tableObj = lookupKeyRead(c->db, c->argv[1]);
    void *replylen = addDeferredMultiBulkLength(c);
    unsigned long numret = 0;

    // Note to use calloc because empty values should hold NULL pointers
    if(need_expand(tableObj)) {
        tableObj->max_length <<= 1;
        char*** migrate_table = calloc(tableObj->max_length, sizeof(sds***));

        if(tableObj->table != NULL) {
            memcpy(migrate_table, tableObj->table, sizeof(sds**)*tableObj->length);
            void* temp = tableObj->table;
            tableObj->table = migrate_table;
            free(temp);
        } else {
            tableObj->table = migrate_table;
        }
    }

    tableObj->table[tableObj->length] = calloc(1, tableObj->column_length * sizeof(char*));

    // Note: argc not necessarily all values to be inserted (could be "column name\rcolumn value")
    if((((char*)c->argv[2]->ptr)[0]) == '\r') {
        for(int i = 2; i < c->argc; i++) {
            const size_t iter = 1;
            tableObj->table[tableObj->length][i-2] = calloc(1, strlen(c->argv[i]->ptr)-iter);
            memcpy(tableObj->table[tableObj->length][i-2], (((char*)c->argv[i]->ptr)+iter), strlen(c->argv[i]->ptr)-iter);
        }
    } else {
        for(int i = 2; i < c->argc; i++) {
            for(int j = 0; j < tableObj->column_length; j++) {
                size_t iter = 0;
                while((((char*)c->argv[i]->ptr)+iter)[0] != '\r') {
                    if((((char*)c->argv[i]->ptr)+iter)[0] != (((char*)tableObj->column[j])+iter)[0]) {
                        break;
                    }
                    iter++;
                }
                if((((char *)c->argv[i]->ptr)+iter)[0] != '\r') continue;
                iter++;
                tableObj->table[tableObj->length][j] = calloc(1, sdslen(c->argv[i]->ptr)-iter);
                memcpy(tableObj->table[tableObj->length][j], (((char*)c->argv[i]->ptr)+iter), sdslen(c->argv[i]->ptr)-iter);
            }
        }
    }

    for(int i = 0; i < tableObj->column_length; i++) {
        if(tableObj->table[tableObj->length][i] != NULL) {
            addReplyBulkCBuffer(c, tableObj->table[tableObj->length][i], strlen(tableObj->table[tableObj->length][i]));
        } else {
            tableObj->table[tableObj->length][i] = calloc(1, sizeof("null"));
            memcpy(tableObj->table[tableObj->length][i], "null", sizeof("null"));
            addReplyBulkCBuffer(c, "null", sizeof("null")-1);
        }
        numret++;
    }
    
    setDeferredMultiBulkLength(c,replylen,numret);
    tableObj->length++;
}

void relcreateCommand(client* c) {
    //robj *arg_table = c->argv[1];
    //arg_table = getDecodedObject(arg_table);
    //sds* table = malloc(1, sdslen(arg_table));

    //robj* tableObj = calloc(1, sizeof(robj));
    robj* tableObj = createRawStringObject(c->argv[1]->ptr, sdslen(c->argv[1]->ptr));
    tableObj->max_length = 4;
    tableObj->column_length = (c->argc - 2)/2;
    tableObj->column = calloc(1, ((c->argc-2)/2)*sizeof(char*));
    tableObj->col_type = calloc(1, ((c->argc-2)/2)*sizeof(char*));

    for(int i = 2; i <= (c->argc)/2; i++) {
        robj *arg_column = c->argv[i];
        incrRefCount(arg_column);
        tableObj->column[i-2] = arg_column->ptr;
    }
    for(int i = (c->argc)/2+1; i < c->argc; i++) {
        robj *arg_col_type = c->argv[i];
        incrRefCount(arg_col_type);
        tableObj->col_type[i-((c->argc)/2+1)] = arg_col_type->ptr;
    }

    tableObj->table = calloc(tableObj->column_length, sizeof(char**)*tableObj->max_length);
    // c->argv[1] copied in dictAddRaw, decrease reference to arg_table (if ref == 0 then freed)
    setKey(c->db, c->argv[1], tableObj);

    addReplyMultiBulkLen(c, 1);
    addReplyBulkCBuffer(c, "OK", sizeof("OK")-1);
}

void relupdateCommand(client* c) {
    robj* tableObj = lookupKeyRead(c->db, c->argv[1]);
    char* set = c->argv[2]->ptr;
    char* cond = c->argv[3]->ptr;

    for(int i = 0; i < tableObj->length; i++) {
        fprintf(stderr, "main for loop for update command\n");
        if(parse_where(cond, strlen(cond), tableObj, NULL, i, 0)) {
            parse_set(set, tableObj, i, 0);
        }
    }
    fprintf(stderr, "escaped from main for loop\n");
    addReplyMultiBulkLen(c, 1);
    addReplyBulkCBuffer(c, "OK", sizeof("OK")-1);
    return;
}

void reldeleteCommand(client* c) {
    robj* tableObj = lookupKeyRead(c->db, c->argv[1]);
    const char* cond = c->argc > 2 ? c->argv[2]->ptr : NULL;

    if(*cond == NULL) {
        tableObj->max_length = 4;

        for(int i = 0; i < tableObj->length; i++) {
            for(int j = 0; j < tableObj->column_length; j++) {
                free(tableObj->table[i][j]);
            }
            free(tableObj->table[i]);
        }

        free(tableObj->table);
        tableObj->table = calloc(tableObj->column_length, sizeof(char**)*tableObj->max_length);
        tableObj->length = 0;

        addReplyMultiBulkLen(c, 1);
        addReplyBulkCBuffer(c, "OK", sizeof("OK")-1);
        return;
    }

    int *row_mark = calloc(1, (tableObj->length) * sizeof(int));
    unsigned int new_size = tableObj->length;

    for(int i = 0; i < tableObj->length; i++) {
        if(parse_where(cond, strlen(cond), tableObj, NULL, i, 0)) {
            for(int j = 0; j < tableObj->column_length; j++) {
                free(tableObj->table[i][j]);
            }
            free(tableObj->table[i]);
            new_size--;
            row_mark[i] = 1;
        }
    }

    // Note: max_length = 4 at minimum
    unsigned int new_size_round_up = 2;
    do {
        new_size_round_up <<= 1;
    } while(new_size_round_up < new_size);

    char ***temp = tableObj->table;
    tableObj->table = calloc(new_size_round_up, sizeof(char**));
    tableObj->max_length = new_size_round_up;

    unsigned int iter = 0;
    for(int i = 0; i < tableObj->length; i++) {
        if(!row_mark[i]) {
            tableObj->table[iter] = temp[i];
            fprintf(stderr, "table[%d][1] = %s\n", iter, tableObj->table[iter][1]);
            iter++;
        }
    }
    tableObj->length = iter;
    free(temp);
    free(row_mark);
            
    addReplyMultiBulkLen(c, 1);
    addReplyBulkCBuffer(c, "OK", sizeof("OK")-1);
}


void relselectCommand(client* c) {
    robj* tableObj = lookupKeyRead(c->db, c->argv[1]);

    if(tableObj->length == 0) {
        addReplyMultiBulkLen(c, 1);
        addReplyBulkCBuffer(c, "Empty", sizeof("Empty")-1);
        return;
    }

    void *replylen = addDeferredMultiBulkLength(c);

    char* table_column_argv[100];
    int table_column_size[100];
    // argument to index
    int a2i[100];

    int table_is_sum[100];
    int table_is_count[100];
    memset(table_is_sum, 0, sizeof(int) * 100);
    memset(table_is_count, 0, sizeof(int) * 100);

    int global_is_sum = 0;
    int global_is_count = 0;

    int group_count = 0;

    char *where_clause = c->argc > 3 ? c->argv[3]->ptr : NULL;
    if(c->argc > 3) {
        if(*(char*)(where_clause) == NULL)
            where_clause = NULL;
    }

    char *group_target = c->argc > 4 ? c->argv[4]->ptr : NULL;
    if(c->argc > 5) {
        if(*(char*)(c->argv[4]->ptr) == NULL)
            group_target = NULL;
    }
    int group_target_idx = -1;
    if(group_target != NULL) {
        group_target_idx = get_col_index(tableObj, group_target);
    }

    char *having_clause = c->argc > 5 ? c->argv[5]->ptr : NULL;
    if(c->argc > 5) {
        if(*(char*)(c->argv[5]->ptr) == NULL)
            having_clause = NULL;
    }
    int having_literal = -1;
    int is_having_count = 0, is_having_sum = 0;
    int having_col_idx = -1;
    if(having_clause != NULL) {
        having_literal = get_having_literal(having_clause);
        if((*(having_clause+1) == 'c' && *(having_clause+2) == ':') ||
                (*(having_clause+2) == 'c' && *(having_clause+3) == ':')) {
            is_having_count = 1;
            char* col = calloc(100, sizeof(char));
            size_t len = 0;
            while(*(having_clause+len) != '#' && *(having_clause+len) != '"')
                len++;
            size_t sub = 0;
            while(*(having_clause+sub) != ':')
                sub++;
            sub++;
            len = len - sub;
            memcpy(col, having_clause+sub, len);
            having_col_idx = get_col_index(tableObj, col);
        }
        else if((*(having_clause+1) == 's' && *(having_clause+2) == ':') ||
                (*(having_clause+2) == 's' && *(having_clause+3) == ':')) {
            is_having_sum = 1;
        }
    }

    int table_column_argc = 0;
    size_t current_size = 0, numret = 0;

    // "table.column"s argument
    //char* iter = (char *)(c->argv[1]);
    robj* columns = c->argv[2];
    columns = getDecodedObject(columns);
    char* iter = (char *)(columns->ptr);
    char* past_iter = iter;

    int is_select_all = 0;
    if(*iter == ':' && *(iter+1) == '*') {
        is_select_all = 1;
        iter += 2;
    }

    while (*iter != '\0') {
        if(*iter == 'c' && *(iter+1) == ':') {
            global_is_count |= table_is_count[table_column_argc] = 1;
            past_iter = iter += 2;
        } else if(*iter == 's' && *(iter+1) == ':') {
            global_is_sum   |= table_is_sum[table_column_argc] = 1;
            past_iter = iter += 2;
        } else if(*iter == ':') {
            past_iter = iter += 1;
        }
        if(*iter == '\r') {
            table_column_argv[table_column_argc] = (char*)calloc(current_size, sizeof(char));
            // Note: Non-NULL terminated
            memcpy(table_column_argv[table_column_argc], past_iter, current_size);
            a2i[table_column_argc] = get_col_index(tableObj, table_column_argv[table_column_argc]);
             
            table_column_size[table_column_argc++] = current_size;
            current_size = 0;
            past_iter = ++iter;
            continue;
        }
        iter++;
        current_size++;
    }

    table_column_argv[table_column_argc] = (char*)calloc(current_size, sizeof(char));

    // Note: Non-NULL terminated
    memcpy(table_column_argv[table_column_argc], past_iter, current_size);
    a2i[table_column_argc] = get_col_index(tableObj, table_column_argv[table_column_argc]);
    table_column_size[table_column_argc++] = current_size;

    if(group_target_idx >= 0) {
        quicksort_by_column(tableObj->table, tableObj->length, group_target_idx);
    }

    // if select *
    if(is_select_all) {
        char *group_distinct_iter = tableObj->table[0][group_target_idx];

        addReplyLongLong(c, tableObj->column_length); numret++;
        for(int i = 0; i < tableObj->length; i++) {
            int is_distinct = 0;
            if(group_target_idx >= 0 && strcmp(group_distinct_iter, tableObj->table[(i+1)%(tableObj->length)][group_target_idx]) != 0) 
                is_distinct = 1;
                
            if((where_clause != NULL && parse_where(where_clause, strlen(where_clause), tableObj, NULL, i, 0)) ||
                    where_clause == NULL) {
                for(int j = 0; j < tableObj->column_length; j++) {
                    if((group_target_idx >= 0 && is_distinct) || group_target_idx < 0) {
                        addReplyBulkCBuffer(c, tableObj->table[i][j], strlen(tableObj->table[i][j]));
                        numret++;
                    }
                }
            }
            if(is_distinct) {
                group_distinct_iter = tableObj->table[(i+1)%(tableObj->length)][group_target_idx];
            }
        }
    } 
    // if select columns
    else {
        char *group_distinct_iter = tableObj->table[0][group_target_idx];

        long long sum[100], count[100];
        memset(sum, 0, 100*sizeof(long long));
        memset(count, 0, 100*sizeof(long long));

        long long h_sum = 0, h_count = 0;

        addReplyLongLong(c, table_column_argc); numret++;
        for(int i = 0; i < tableObj->length; i++) {
            int is_distinct = 0;
            if(group_target_idx >= 0 && strcmp(group_distinct_iter, tableObj->table[(i+1)%(tableObj->length)][group_target_idx]) != 0) 
                is_distinct = 1;
                
            if((where_clause != NULL && parse_where(where_clause, strlen(where_clause), tableObj, NULL, i, 0)) ||
                    where_clause == NULL) {
                for(int j = 0; j < table_column_argc; j++) {
                    if(table_is_sum[j]) sum[j] += atoi(tableObj->table[i][a2i[j]]);
                    if(table_is_count[j]) count[j]++;
                    if(having_col_idx == a2i[j]) {
                        fprintf(stderr, "j = %d, i = %d, table[%d][%d] = %s\n", j, i, i, a2i[j], tableObj->table[i][a2i[j]]);
                        if(is_having_count) h_count++;
                        else h_sum += atoi(tableObj->table[i][a2i[j]]);
                    }

                    if((group_target_idx >= 0 && is_distinct) || group_target_idx < 0) {
                        if(having_clause == NULL) {
                            if(table_is_sum[j])
                                addReplyLongLong(c, sum[j]);
                            else if(table_is_count[j])
                                addReplyLongLong(c, count[j]);
                            else
                                addReplyBulkCBuffer(c, tableObj->table[i][a2i[j]], strlen(tableObj->table[i][a2i[j]]));
                            fprintf(stderr, tableObj->table[i][a2i[j]]);
                            numret++;
                            sum[j] = count[j] = 0;
                        }
                        else{
                            if(group_target_idx >= 0) {
                                if((is_having_sum && having_op(having_clause, h_sum, having_literal)) ||
                                        (is_having_count && having_op(having_clause, h_count, having_literal))) {
                                    if(table_is_sum[j])
                                        addReplyLongLong(c, sum[j]);
                                    else if(table_is_count[j])
                                        addReplyLongLong(c, count[j]);
                                    else
                                        addReplyBulkCBuffer(c, tableObj->table[i][a2i[j]], strlen(tableObj->table[i][a2i[j]]));
                                    sum[j] = count[j] = 0;
                                    numret++;
                                }
                            }
                        }
                    }
                }
            }

            if(is_distinct) {
                group_distinct_iter = tableObj->table[(i+1)%(tableObj->length)][group_target_idx];
                h_sum = h_count = 0;
                for(int j = 0; j < table_column_argc; j++) {
                    sum[j] = count[j] = 0;
                }
            }
        }
    }

    for(int i = 0; i < table_column_argc; i++) {
        free(table_column_argv[i]);
    }

    decrRefCount(columns);
    setDeferredMultiBulkLength(c,replylen,numret);
    return;
}

int get_having_literal(char *cond) {
    int ret = -1;
    char *iter = cond;
    while(*iter != '#' && *iter != '"')
        iter++;
    iter++;
    ret = atoi(iter);
    return ret;
}

int equal_less(long long sum_or_count, int literal) {
    return sum_or_count <= literal;
}

int equal(long long sum_or_count, int literal) {
    return sum_or_count == literal;
}

int equal_greater(long long sum_or_count, int literal) {
    return sum_or_count >= literal;
}

int not_equal(long long sum_or_count, int literal) {
    return !(sum_or_count == literal);
}

int less(long long sum_or_count, int literal) {
    return sum_or_count < literal;
}

int greater(long long sum_or_count, int literal) {
    return sum_or_count > literal;
}

int having_op(char *cond, long long sum_or_count, int literal) {
    char* iter = cond;
    while(*iter != '<' && *iter != '=' && *iter != '>'
            && *iter != '!')
        iter++;

    switch(*iter) {
        case '<':
            if(*(iter+1) == '=')
                return equal_less(sum_or_count, literal);
            else
                return less(sum_or_count, literal);
            break;
        case '>':
            if(*(iter+1) == '=')
                return equal_greater(sum_or_count, literal);
            else
                return greater(sum_or_count, literal);
            break;
        case '=':
            return equal(sum_or_count, literal);
            break;
        case '!':
            if(*(iter+1) == '=')
                return not_equal(sum_or_count, literal);
        default:
            fprintf(stderr, "wrong type of operator %c in parse_have\n", *cond);
            exit(1);
    }
}
 
/*
int eval_having_cond(char *cond, long long sum_or_count, int literal) {
    switch(*cond) {
        case '<':
            //if(*(cond+1) == '=')

            //else if(*(cond+1) == 'c' && *(cond+2) == ':')
            break;
        case '>':
            break;
        case '=':
            break;
        case '!':
            break;
        default:
            fprintf(stderr, "wrong type of operator %c in parse_have\n", *cond);
            exit(1);
    }

    if(*cond == 'c' && *(cond+1) == ':') {

    }
    else if(*cond == 's' && *(cond+1) == ':') {
    
    }
}
*/
