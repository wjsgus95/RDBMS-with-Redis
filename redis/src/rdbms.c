#include "server.h"
#include <stdio.h>

void relselectCommand(client* c) {
    robj *o;

    printf("relselect called\n");
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL) return;
}
