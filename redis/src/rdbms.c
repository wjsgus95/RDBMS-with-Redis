#include "server.h"
//#include <stdio.h>

#define need_expand(x) ((x->length >= x->max_length))

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
    robj* tableObj = lookupKeyRead(c->argv[1]);

    if(need_expand(tableObj)) {
        tableObj->max_length <<= 1;
        sds*** migrate_table = calloc(tableObj->max_length, sizeof(sds***));
        //sds** migrate_column = calloc(tableObj->column_length, sizeof(sds**));

        if(tableObj->table != NULL) {
            memcpy(migrate_table, tableObj->table, sizeof(sds**)*tableObj->length);
            //memcpy(migrate_column, tableObj->column, sizeof(sds*)*tableObj->column_length);
        }
    }

    tableObj->table[length] = calloc(1, tableObj->column_length);

    for(int i = 2; i < c->argc; i++) {
    }

    for(int i = 2; i < c->argc; i++) {
        tableObj->table[length][i-2] = calloc(1, sdslen(c->argv[i]));
        memcpy(tableObj->table[length][i-2], c->argv[i], sdslen(c->argv[i]));
    }
}

void relcreateCommand(client* c) {
    robj *arg_table = c->argv[1];
    arg_table = getDecodedObject(arg_table);
    //sds* table = malloc(1, sdslen(arg_table));

    rTable* tableObj = calloc(1, sizeof(rTable));
    tableObj->max_length = 4;
    tableObj->column_length = c->argc - 2;
    tableObj->column = calloc(1, (c->argc-2)*sizeof(sds*));

    for(int i = 2; i <= (c->argc)/2; i++) {
        robj *arg_column = c->argv[1];
        arg_column = getDecodedObject(arg_column);
        //tableObj->column[i-2] = calloc(1, size*sizeof(sds));
        // directly assigned but object created in getDecodedObject
        tableObj->column[i-2] = arg_column;
        //size_t size = sdslen(arg_column);
        //memcpy(ctableObj->column[i-2], arg_column, size);
        //decrRefCount(arg_column);
    }
    for(int i = (c->argc)/2+1; i < c->argc; i++) {
        robj *arg_column = c->argv[1];
        arg_column = getDecodedObject(arg_column);
        tableObj->column[i-2] = arg_column;
    }

    tableObj->table = calloc(tableObj->column_length, sizeof(sds**)*tableObj->max_length);
    //c->argv[1] copied in dictAddRaw, decrease reference to arg_table (if ref == 0 then freed)
    setKey(c->db, c->argv[1], tableObj);
    decrRefCount(arg_table);

    addReplyMultiBulkLen(c, 1);
    //queueCommand(c);
    addReplyBulkCBuffer(c, "OK", sizeof("OK"));
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
