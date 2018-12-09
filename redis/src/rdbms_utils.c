#include "server.h"

typedef struct where_return{
    int boolean;
    int offset;
}whereReturn;

int get_col_index(robj* table, char* colname){
    // linear search
    int i = 0;
    for(i; i < table->length; ++i){
        if (strcmp(colname, table->column[0])== 0){
            return i;
        }
    }
    return -1;

}

int get_index(char* str, char){
    char* ptr;
    int index;

    ptr = strchr(str, c);
    if (ptr == NULL)
    {
        //printf("Character not found\n");
        return -1;
    }

    index = ptr - str;

    return index;

}

char* parse_col_name_where(robj* tableObj1, robj* tableObj2, char* colname){
    first_idx = get_col_index(tableObj1, colname);
    second_idx = get_col_index(tableObj2, colname);
}

int parse_terminal_size(char* str, int offset){
    char* iter = str + offset;
    size_t length = 0;
    while(*iter != '#' && *iter != '\"' && *iter != '\r'){
        length++;
        iter++;
    }
    return length;
}
/*
whereReturn parse_where(char* str, size_t len, robj* tableObj1, robj* tableObj2, int idx1, int idx2){
    char* iter = str;
    int negation = 0;
    int length1, length2;
    if(tableObj2== NULL){
        switch(*iter){
            case '<':
                if (*(iter+1) == '='){
                }
                else{
                    parse_terminal_size(str, iter - str)
                }
                break;
            case '!':
                break;
            case '=':
                break;
            case '>':
                if (*(iter+1) == '='){
                }
                break;
            case '*':
                break;
        }
        if(negation){
            return
        }
    }
}
*/

char* conditional_operators[] = {
    "=", ">", "<", "!=", "<=", ">=", "*"
};

int parse_conditional_op(char* str, int* header_point){
    int op_length = 1;
    char* op;
    int i = 0;
    int cond_op_idx;

    // extract the operator
    if str[*(header_point+1)] == '='{
        op_length++;
    }
    *header_point = *header_point + op_length;
    op = (char*) calloc(1, op_length * sizeof(char));
    for (i=0; i<op_length; ++i){
        op[i] = str[i];
    }

    // find where the operator exists
    for (i=0 ; i < 7 ; ++i){
        if (strcmp(op, conditional_operators[i]) == 0){
            return i;
        }
    }
    fprintf(stderr, "this operator is not supported");
    return -1;
}

int parse_primary(char* str, int* header_point, size_t len, robj* tableObj1, robj* tableObj2, int idx1, int idx2){
    char curr_val = str[*header_point];
    char* str1 = NULL;
    char* str2 = NULL;
    int length1 = 0;
    int length2 = 0;
    char op_type;
    int ret_val;

    *header_point = *header_point + 1;
    if (curr_val == '\0'){
        return 1;
    }
    // obtain the operator
    op = parse_conditional_op(str, header_point);

    // obtain the first operend and its type
    while(*((char*)header_point) != '#' && *((char*)header_point) != '\"'){
        length1++;
        *header_point = *header_point + 1;
    }
    str1 = (char*) calloc(1, length1 + 1);
    op_type = str[*header_point];
    *header_point = *header_point + 1;

    // obtain the second operend
    while(*((char*)header_point) != '\r'){
        length1++;
        *header_point = *header_point + 1;
    }
    str2 = (char*) calloc(1, length2 + 1);
    *header_point = *header_point + 1;

    // run operation
    ret_val = run_where_unit_op(op, str1, str2, op_type, tableObj1, tableObj2, idx1, idx2);

    // release memory
    free(str1);
    free(str2);
}

int parse_where_recursive(char* str, int* header_point, size_t len, robj* tableObj1, robj* tableObj2, int idx1, int idx2){
    char curr_val = str[*header_point];
    int val1, val2;

    *header_point = *header_point + 1;
    if (curr_val == '\0'){
        return 1;
    }

    // obtain the operator
    op = str[*header_point];

    *header_point = *header_point + 1;



}


int parse_where(char* str, size_t len, robj* tableObj1, robj* tableObj2, int idx1, int idx2){
    int* header_point = (int *) malloc(sizeof(int)); // psudo global
    int ret_val = parse_where_recursive(str, header_point, len, tableObj1, tableObj2, idx1, idx2);
    free(header_point);
    return ret_val
}
