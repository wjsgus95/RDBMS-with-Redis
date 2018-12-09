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

    // important to use calloc because empty values should hold NULL pointers
    if(need_expand(tableObj)) {
        tableObj->max_length <<= 1;
        char*** migrate_table = calloc(tableObj->max_length, sizeof(sds***));
        //sds** migrate_column = calloc(tableObj->column_length, sizeof(sds**));

        if(tableObj->table != NULL) {
            memcpy(migrate_table, tableObj->table, sizeof(sds**)*tableObj->length);
            //memcpy(migrate_column, tableObj->column, sizeof(sds*)*tableObj->column_length);
            void* temp = tableObj->table;
            tableObj->table = migrate_table;
            free(temp);
        } else {
            tableObj->table = migrate_table;
        }
    }

    fprintf(stderr, "columns\n");
    fprintf(stderr, "%s %s\n", tableObj->column[0], tableObj->column[1]);
    fprintf(stderr, "%d %d\n", tableObj->length, tableObj->column_length);
    fprintf(stderr, "%d\n", need_expand(tableObj));

    tableObj->table[tableObj->length] = calloc(1, tableObj->column_length);

    // Note: argc not necessarily all values to be inserted (could be "column name\rcolumn value")
    fprintf(stderr, "c->argv[2]->ptr = %c\n", ((char*)c->argv[2]->ptr)[0]);
    fprintf(stderr, "c->argv[2]->ptr = %s\n", c->argv[2]->ptr);
    if((((char*)c->argv[2]->ptr)[0]) == '\r') {
        fprintf(stderr, "column unspecified\n");
        for(int i = 2; i < c->argc; i++) {
            const size_t iter = 1;
            tableObj->table[tableObj->length][i-2] = calloc(1, sdslen(c->argv[i]->ptr)-iter);
            memcpy(tableObj->table[tableObj->length][i-2], (((char*)c->argv[i]->ptr)+iter), sdslen(c->argv[i]->ptr)-iter);
        }
    } else {
        for(int i = 2; i < c->argc; i++) {
            for(int j = 0; j < tableObj->column_length; j++) {
                size_t iter = 0;
                while((((char*)c->argv[i]->ptr)+iter)[0] != '\r') {
                    fprintf(stderr, "first in while %c\n", (((char*)c->argv[i]->ptr)+iter)[0]);
                    if((((char*)c->argv[i]->ptr)+iter)[0] != (((char*)tableObj->column[j])+iter)[0]) {
                        fprintf(stderr, "in if in while %c %c\n", (((char*)c->argv[i]->ptr)+iter)[0], *(((char*)tableObj->column[j])+iter));
                        break;
                    }
                    iter++;
                }
                if((((char *)c->argv[i]->ptr)+iter)[0] != '\r') continue;
                iter++;
                fprintf(stderr, "in if in while %c %c\n", (((char*)c->argv[i]->ptr)+iter)[0], *(((char*)tableObj->column[j])+iter));
                fprintf(stderr, "column match: %s\n", tableObj->column[j]);
                tableObj->table[tableObj->length][j] = calloc(1, sdslen(c->argv[i]->ptr)-iter);
                memcpy(tableObj->table[tableObj->length][j], (((char*)c->argv[i]->ptr)+iter), sdslen(c->argv[i]->ptr)-iter);
            }
        }
    }

    //addReplyMultiBulkLen(c, tableObj->column_length);
    //queueCommand(c);
    for(int i = 0; i < tableObj->column_length; i++) {
        if(tableObj->table[tableObj->length][i] != NULL) {
            addReplyBulkCBuffer(c, tableObj->table[tableObj->length][i], strlen(tableObj->table[tableObj->length][i]));
        } else {
            addReplyBulkCBuffer(c, "null", sizeof("null")-1);
        }
        numret++;
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

    char* table_column_argv[100];
    int table_column_size[100];
    int table_column_argc = 0;
    size_t current_size = 0;

    // "table.column"s argument
    //char* iter = (char *)(c->argv[1]);
    robj* value = c->argv[1];
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
        
    decrRefCount(value);
    for(int i = 0; i < table_column_argc; i++) {
        free(table_column_argv[i]);
    }
    return;
}
