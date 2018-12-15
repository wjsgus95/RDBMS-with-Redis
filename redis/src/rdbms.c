#include "server.h"

#define need_expand(x) ((((x)->length >= (x)->max_length)))
#define group_need_expand(x) ((((x)->group_length >= (x)->group_max_length)))

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
    const char* set = c->argv[2]->ptr;
    const char* cond = c->argv[3]->ptr;

    for(int i = 0; i < tableObj->length; i++) {
        if(parse_where(cond, strlen(cond), tableObj, NULL, i, 0)) {
            parse_set(set, tableObj, i, 0);
        }
    }
}

void reldeleteCommand(client* c) {
    robj* tableObj = lookupKeyRead(c->db, c->argv[1]);
    const char* cond = c->argc > 2 ? c->argv[2]->ptr : NULL;
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
    void *replylen = addDeferredMultiBulkLength(c);

    char* table_column_argv[100];
    int table_column_size[100];

    int table_is_sum[100];
    int table_is_count[100];
    memset(table_is_sum, 0, sizeof(int) * 100);
    memset(table_is_count, 0, sizeof(int) * 100);

    int global_is_sum = 0;
    int global_is_count = 0;

    int group_count = 0;
    // Note: c->argc > 4 iff group by is given
    char *group_target = c->argc > 4 ? c->argv[4]->ptr : NULL;
    int group_target_idx = -1;
    if(group_target != NULL) {
        group_target_idx = get_col_index(tableObj, group_target);
    }
    char *having_clause = c->argc > 5 ? c->argv[5]->ptr : NULL;


    int table_column_argc = 0;
    size_t current_size = 0, numret = 0;

    // "table.column"s argument
    //char* iter = (char *)(c->argv[1]);
    robj* columns = c->argv[2];
    columns = getDecodedObject(columns);
    char* iter = (char *)(columns->ptr);
    char* past_iter = iter;

    int is_select_all = 0;
    if(*iter == ':' && *(iter+1) == '*')
        is_select_all = 1;

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
    table_column_size[table_column_argc++] = current_size;

    if(group_target_idx >= 0) {
        quicksort_by_column(tableObj->table, tableObj->length, group_target_idx);
    }

    // if select *
    if(is_select_all) {
        char *group_distinct_iter = tableObj->table[0][group_target_idx];

        addReplyLongLong(c, tableObj->column_length); numret++;
        //for(int i = 0; i < tableObj->column_length; i++) {
        //    for(int j = 0; j < tableObj->length; j++) {
        //        if((c->argc > 3 && parse_where(c->argv[3]->ptr, strlen(c->argv[3]->ptr), tableObj, NULL, j, 0)) ||
        //                c->argc <= 3) {
        //            addReplyBulkCBuffer(c, tableObj->table[j][i], strlen(tableObj->table[j][i]));
        //            numret++;
        //        }
        //    }
        //}
        for(int i = 0; i < tableObj->length; i++) {
            int is_distinct = 0;
            if(group_target_idx >= 0 && strcmp(group_distinct_iter, tableObj->table[(i+1)%(tableObj->length)][group_target_idx]) != 0) 
                is_distinct = 1;
                
            if((c->argc > 3 && parse_where(c->argv[3]->ptr, strlen(c->argv[3]->ptr), tableObj, NULL, i, 0)) ||
                    c->argc <= 3) {
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

        addReplyLongLong(c, table_column_argc); numret++;
        for(int i = 0; i < tableObj->length; i++) {
            int is_distinct = 0;
            if(group_target_idx >= 0 && strcmp(group_distinct_iter, tableObj->table[(i+1)%(tableObj->length)][group_target_idx]) != 0) 
                is_distinct = 1;
                
            if((c->argc > 3 && parse_where(c->argv[3]->ptr, strlen(c->argv[3]->ptr), tableObj, NULL, i, 0)) ||
                    c->argc <= 3) {
                for(int j = 0; j < table_column_argc; j++) {
                    for(int k = 0; k < tableObj->column_length; k++) {
                        if(strcmp(tableObj->column[k], table_column_argv[j]) == 0) {
                            if(table_is_sum[j]) sum[j] += atoi(tableObj->table[i][k]);
                            if(table_is_count[j]) count[j]++;

                            if((group_target_idx >= 0 && is_distinct) || group_target_idx < 0) {
                                if(table_is_sum[j])
                                    addReplyLongLong(c, sum[j]);
                                else if(table_is_count[j])
                                    addReplyLongLong(c, count[j]);
                                else
                                    addReplyBulkCBuffer(c, tableObj->table[i][k], strlen(tableObj->table[i][k]));
                                numret++;
                                sum[j] = count[j] = 0;
                            }
                        }
                    }
                }
            }

            if(is_distinct) {
                group_distinct_iter = tableObj->table[(i+1)%(tableObj->length)][group_target_idx];
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


int parse_having(char *cond, robj* tableObj, int row_idx, long long sum, long long count) {
    switch(*cond) {
        case '<':
            if(*(cond+1) == 'c' && *(cond+2) == ':')
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
