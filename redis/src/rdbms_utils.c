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

int equal_to(char* val1, char* val2){
    if (strcmp(val1, val2) == 0){
        return 1;
    }
    else{
        return 0;
    }
}

int greater_than(char* val1, char* val2, char* type){
    // in case of type == "int"
    if (strcmp(type, "int") == 0){
        // type conversion from str to int
        if (atoi(val1) > atoi(val2)){
            return 1;
        }
        else{
            return 0;
        }
    }

    // otherwise, val1 and val2 are strings
    if (strcmp(val1, val2) > 0){
        return 1;
    }
    else{
        return 0;
    }
}

int like(char* regexp, char* text){
    if (regexp[0] == '\0')
        return 1;
    if (regexp[1] == '%')
        return like_star(regexp[0], regexp+2, text);
    if (*text!='\0' && (regexp[0]=='_' || regexp[0]==*text))
        return like(regexp+1, text+1);
    return 0;
}

int like_star(char c, char* regexp, char* text){
    do {
        if (like(regexp, text))
            return 1;
    } while (*text != '\0' && (*text++ == c || c == '.'));
    return 0;
}

int get_col_idx(char* col_query, char** col, int n_cols){
    int i;
    char* curr_col;
    for(i=0; i<n_cols; ++i){
        curr_col = col[i];
        if (strcmp(curr_col, col_query) == 0){
            return i;
        }
    }
    return -1;
}

char* get_row_val(char* col_query, robj* tableObj, int row_idx){
    int col_idx;
    int n_cols = tableObj->column_length;
    char** col = tableObj->column;
    col_idx = get_col_idx(col_query, col, n_cols);
    if (col_idx == -1){
        //fprintf(stderr, "wrong column name %s\n", col_query);
        return 0;
    }
    //char** row = (char***) tableObj->table + row_idx;
    //return row[col_idx];
    return tableObj->table[row_idx][col_idx];
}


int run_unit_cond_op(int op, char* param1, char* param2, char op_type, robj* tableObj1, robj* tableObj2, int idx1, int idx2){
    // to be implemented: cartesian product
    char *val1, *val2, *col_type;
    val1 = get_row_val(param1, tableObj1, idx1);
    if (op_type == '#'){
        val2 = get_row_val(param2, tableObj1, idx1);
    }
    else{
        val2 = param2;
    }
    col_type = tableObj1->col_type[get_col_idx(param1, tableObj1->column, tableObj1->column_length)];
    switch(op){
        case 0: // =
            return equal_to(val1, val2);
            break;
        case 1: // >
            return greater_than(val1, val2, col_type);
            break;
        case 2: // <
            return greater_than(val2, val1, col_type);
            break;
        case 3: // !=
            return !(equal_to(val1, val2));
            break;
        case 4: // <=
            return (greater_than(val2, val1, col_type) || equal_to(val1, val2));
            break;
        case 5: // >=
            return (greater_than(val1, val2, col_type) || equal_to(val1, val2));
            break;
        case 6: // * (like)
            return like(val2, val1);
            break;
        default:
            fprintf(stderr, "unknown conditional operator %d\n", op);
            break;
    }
    return -1;

}

int parse_conditional_op(char* str, int* header_point){
    int op_length = 1;
    char* op;
    int i = 0;
    int cond_op_idx;

    // extract the operator
    if (str[*(header_point)+1] == '='){
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
            free(op);
            return i;
        }
    }
    free(op);
    return -1;
}

int parse_primary(char* str, int* header_point, size_t len, robj* tableObj1, robj* tableObj2, int idx1, int idx2){
    char* str1 = NULL;
    char* str2 = NULL;
    int length1 = 0;
    int length2 = 0;
    char op_type;
    int ret_val, op;

    // obtain the operator
    op = parse_conditional_op(str, header_point);

    // obtain the first operend and its type
    while(str[*header_point] != '#' && str[*header_point] != '\"'){
        length1++;
        *header_point = *header_point + 1;
    }
    str1 = (char*) calloc(1, length1 + 1);
    memcpy(str1, (str+*(header_point))-length1, length1);
    op_type = str[*header_point];
    *header_point = *header_point + 1;

    // obtain the second operend
    while(str[*header_point] != '\r' && str[*header_point] != '\0'){
        length2++;
        *header_point = *header_point + 1;
    }
    str2 = (char*) calloc(1, length2 + 1);
    memcpy(str2, (str+*(header_point))-length2, length2);
    *header_point = *header_point + 1;

    // run operation
    ret_val = run_unit_cond_op(op, str1, str2, op_type, tableObj1, tableObj2, idx1, idx2);

    // release memory
    free(str1);
    free(str2);
    return ret_val;
}

int parse_where_recursive(char* str, int* header_point, size_t len, robj* tableObj1, robj* tableObj2, int idx1, int idx2){
    char op = str[*header_point];
    int val1, val2;
    char next_op;

    *header_point = *header_point + 1;
    if (op == '\0'){
        return 1;
    }
    else if (op == '<' || op == '>' || op == '=' || op == '*' || op == '!'){
        *header_point = *header_point - 1;
        return parse_primary(str, header_point, len, tableObj1, tableObj2, idx1, idx2);
    }

    // run first evaluation
    // check next op
    next_op = str[*header_point];
    if (next_op == "&" || next_op == '|'){
        val1 = parse_where_recursive(str, header_point, len, tableObj1, tableObj2, idx1, idx2);
    }
    else if (next_op == '<' || next_op == '>' || next_op == '=' || next_op == '*' || next_op == '!'){
        val1 = parse_primary(str, header_point, len, tableObj1, tableObj2, idx1, idx2);
    }
    else{
        fprintf(stderr, "wrong type token in %s\n", str);
        return -1;
    }

    // run second evaluation
    // check next op
    next_op = str[*header_point];
    if (next_op == "&" || next_op == '|'){
        val2 = parse_where_recursive(str, header_point, len, tableObj1, tableObj2, idx1, idx2);
    }
    else if (next_op == '<' || next_op == '>' || next_op == '=' || next_op == '*' || next_op == '!'){
        val2 = parse_primary(str, header_point, len, tableObj1, tableObj2, idx1, idx2);
    }
    else{
        fprintf(stderr, "wrong type token in %s, when header=%d\n", str, *header_point);
        return -1;
    }

    // evaluate
    if (op == '&'){
        return val1 & val2;
    }
    else if (op == '|'){
        return val1 | val2;
    }
    else{
        fprintf(stderr, "wrong type operator %c in %s, when header=%d\n", op, str, *header_point);
        return -1;
    }

}


int parse_where(char* str, size_t len, robj* tableObj1, robj* tableObj2, int idx1, int idx2){
    int* header_point = (int *) calloc(1, sizeof(int)); // pseudo global
    int ret_val = parse_where_recursive(str, header_point, len, tableObj1, tableObj2, idx1, idx2);
    free(header_point);
    return ret_val;
}
