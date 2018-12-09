#include "server.h"
//#include <stdio.h>

#define need_expand(x) ((((x)->length >= (x)->max_length)))

#define and(x) (x == '&');
#define or(x) (x == '|');

typedef struct rTable {
    sds*** table;
    sds** column;
    sds** type;
    size_t length;
    size_t max_length;
    size_t column_length;
} rTable;

/*
void keysCommand(client *c) {
    dictIterator *di;
    dictEntry *de;
    sds pattern = c->argv[1]->ptr;
    int plen = sdslen(pattern), allkeys;
    unsigned long numkeys = 0;
    void *replylen = addDeferredMultiBulkLength(c);

    di = dictGetSafeIterator(c->db->dict);
    allkeys = (pattern[0] == '*' && pattern[1] == '\0');
    while((de = dictNext(di)) != NULL) {
        sds key = dictGetKey(de);
        robj *keyobj;

        if (allkeys || stringmatchlen(pattern,plen,key,sdslen(key),0)) {
            keyobj = createStringObject(key,sdslen(key));
            if (!keyIsExpired(c->db,keyobj)) {
                addReplyBulk(c,keyobj);
                numkeys++;
            }
            decrRefCount(keyobj);
        }
    }
    dictReleaseIterator(di);
    setDeferredMultiBulkLength(c,replylen,numkeys);
}
*/



void parse_where(char* target, char* clause) {

}


char *operatorTable[] = {
    "=",
    "<=",
    ">=",
    "!=",
    "like",
    "<",
    ">"
};

// = <= >= != like < >
// not done
/*
void* parse_token(char* target, char* string) {
    char* iter = target;
    int count = 0;

    while (end) {
        switch(*iter) {
            case '=':
                break;
            case '<':
                break;
            case '>':
                break;
            case 'l':
                if(strcmp(iter, "like", 4) == 0) {
                    break;
                }
            default:
                break;
        }
        count++;
        iter++;
    }

    if(count > 0) {
        char* token = (char*)malloc(1, count*sizeof(char));
        memcpy(token, target, iter-target);
        return token;
    }
    else return NULL;
}
*/

// not using this for now, incompatible with redis-cluster-py
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

// explicit column specification needs to be handled
void relinsertCommand(client* c) {
    // presumably call by reference
    robj* tableObj = lookupKeyRead(c->db, c->argv[1]);
    void *replylen = addDeferredMultiBulkLength(c);
    unsigned long numret = 0;

    fprintf(stderr, "columns\n");
    fprintf(stderr, "%s %s\n", tableObj->column[0], tableObj->column[1]);
    fprintf(stderr, "%d %d\n", tableObj->length, tableObj->column_length);
    fprintf(stderr, "%d\n", need_expand(tableObj));
    
    // important to use calloc because empty values should hold NULL pointers
    if(need_expand(tableObj)) {
        tableObj->max_length <<= 1;
        sds*** migrate_table = calloc(tableObj->max_length, sizeof(sds***));
        //sds** migrate_column = calloc(tableObj->column_length, sizeof(sds**));

        if(tableObj->table != NULL) {
            memcpy(migrate_table, tableObj->table, sizeof(sds**)*tableObj->length);
            //memcpy(migrate_column, tableObj->column, sizeof(sds*)*tableObj->column_length);
        }
    }

    tableObj->table[tableObj->length] = calloc(1, tableObj->column_length);

    // Note: argc not necessarily all values to be inserted (could be "column name\rcolumn value")
    if(*((char*)c->argv[2]) == '\r') {
        for(int i = 2; i < c->argc; i++) {
            const size_t iter = 1;
            tableObj->table[tableObj->length][i-2] = calloc(1, sdslen(c->argv[i])-iter);
            memcpy(tableObj->table[tableObj->length][i-2], (c->argv[i]+iter), sdslen(c->argv[i])-iter);
        }
    }
    else {
        for(int i = 2; i < c->argc; i++) {
            for(int j = 0; j < tableObj->column_length; j++) {
                size_t iter = 0;
                while(*(((char*)c->argv[i])+iter) != '\r') {
                    if(*(((char*)c->argv[i])+iter) != *(((char*)tableObj->column[j])+iter)) {
                        break;
                    }
                    iter++;
                }
                if(*(((char *)c->argv[i])+iter) != '\r') continue;
                iter++;
                tableObj->table[tableObj->length][j] = calloc(1, sdslen(c->argv[i])-iter);
                memcpy(tableObj->table[tableObj->length][j], (c->argv[i]+iter), sdslen(c->argv[i])-iter);
            }
        }
    }

    //addReplyMultiBulkLen(c, tableObj->column_length);
    //queueCommand(c);
    for(int i = 0; i < tableObj->column_length; i++) {
        //if(tableObj->table[tableObj->length][i] != NULL) {
            //addReplyBulkCBuffer(c, tableObj->table[tableObj->length][i], sdslen(tableObj->table[tableObj->length][i]));
            addReplyBulkCBuffer(c, tableObj->column[i], sdslen(tableObj->column[i]));
            numret++;
        //}
    }
    
    //addReplyBulkCBuffer(c, "not found", sizeof("not found")-1); numret++;
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
        //arg_column = getDecodedObject(arg_column);
        //tableObj->column[i-2] = calloc(1, size*sizeof(sds));
        // directly assigned but object created in getDecodedObject
        incrRefCount(arg_column);
        tableObj->column[i-2] = arg_column->ptr;
        //size_t size = sdslen(arg_column);
        //memcpy(ctableObj->column[i-2], arg_column, size);
        //decrRefCount(arg_column);
    }
    for(int i = (c->argc)/2+1; i < c->argc; i++) {
        robj *arg_col_type = c->argv[i];
        incrRefCount(arg_col_type);
        tableObj->col_type[i-((c->argc)/2+1)] = arg_col_type->ptr;
    }

    tableObj->table = calloc(tableObj->column_length, sizeof(char**)*tableObj->max_length);
    //c->argv[1] copied in dictAddRaw, decrease reference to arg_table (if ref == 0 then freed)
    setKey(c->db, c->argv[1], tableObj);
    //decrRefCount(arg_table);

    addReplyMultiBulkLen(c, 1);
    //queueCommand(c);
    addReplyBulkCBuffer(c, "OK", sizeof("OK")-1);
}

void relupdateCommand(client* c) {

}

void reldeleteCommand(client* c) {

}


void relselectCommand(client* c) {
    robj *o;

    char * table_column_argv[100];
    int table_column_size[100];
    int table_column_argc = 0;
    int current_size = 0;

    // "table.column"s argument
    //char* iter = (char *)(c->argv[1]);
    robj *value = c->argv[1];
    value = getDecodedObject(value);
    char* iter = (char *)(value->ptr);
    char* past_iter = iter;
    while (*iter != '\0') {
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

    // where clause
    // implement = < like
    if(c->argc > 1) {
        c->argv[2];
    }


    /* not working 
    printf("relselect called\n");
    */

    //if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL) return;
    //addReplyMultiBulkLen(c, 2);
    //addReplyBulkCBuffer(c, "woeifj", sizeof("woeifj")-1);
    //addReplyBulkCBuffer(c, "woeifj", sizeof("woeifj")-1);
    //addReplyBulkCBuffer(c, "woeifj", sizeof("woeifj"));

    // temp
    addReplyMultiBulkLen(c, table_column_argc);
    //queueCommand(c);
    for(int i = 0; i < table_column_argc; i++) {
        addReplyBulkCBuffer(c, table_column_argv[i], table_column_size[i]);

    //    c->mstate.commands = zrealloc(c->mstate.commands,
    //    sizeof(multiCmd)*(c->mstate.count+1));
    //    mc = c->mstate.commands+c->mstate.count;
    //    mc->cmd = c->cmd;
    //    mc->argc = c->argc;
    //    mc->argv = zmalloc(sizeof(robj*)*c->argc);
    //    memcpy(mc->argv,c->argv,sizeof(robj*)*c->argc);
    //    for (j = 0; j < c->argc; j++)
    //        incrRefCount(mc->argv[j]);
    //    c->mstate.count++;
    }
        
    for(int i = 0; i < table_column_argc; i++) {
        free(table_column_argv[i]);
    }
    return;
}
