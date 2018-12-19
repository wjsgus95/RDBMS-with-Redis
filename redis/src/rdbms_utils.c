#include "server.h"

typedef struct where_return{
    int boolean;
    int offset;
}whereReturn;

void swap_row(char ***row1, char ***row2) {
    char **temp = *row1;
    *row1 = *row2;
    *row2 = temp;
}

void quicksort_by_column(char*** table, size_t len, int idx) {
    unsigned int i, pvt=0;

    if (len <= 1)
        return;

    // swap a randomly selected value to the last node
    swap_row(table+((unsigned int)rand() % len), table+len-1);

    // reset the pivot index to zero, then scan
    for (i=0;i<len-1;++i)
    {
        if (strcmp(table[i][idx], table[len-1][idx]) < 0)
            swap_row(table+i, table+pvt++);
    }

    // move the pivot value into its place
    swap_row(table+pvt, table+len-1);

    // and invoke on the subsequences. does NOT include the pivot-slot
    quicksort_by_column(table, pvt++, idx);
    quicksort_by_column(table+pvt, len - pvt, idx);
}

int get_col_index(robj* tableObj, char* colname){
    // linear search
    int i = 0;
    char*** table = tableObj->table;
    if(*colname == NULL) return -1;
    for(; i < tableObj->length; ++i){
        if (strcmp(colname, tableObj->column[i])== 0){
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
        if (strtoll(val1, NULL, 10) > strtoll(val2, NULL, 10)){
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

/*
int like(char* regexp, char* text) {
    char* val_iter = text;
    char* re_iter  = regexp;

    fprintf(stderr, "like func start\n");
    fprintf(stderr, "%s\n", val_iter);
    fprintf(stderr, "%s\n", regexp);

    while(*val_iter != '\0' && *re_iter != '\0') {
        if(*re_iter == '%') {
            while(*(re_iter+1) == '%') re_iter++;
            re_iter++;
            while(*val_iter != *re_iter) val_iter++;
        }
        if(*re_iter != '_' && (*re_iter != *val_iter)) return 0;
        //if(*re_iter != *val_iter) return 0;
        re_iter++;
        val_iter++;
    }

    if(*val_iter == '\0' && *re_iter == '\0') return 1;
    else return 0;
}

*/
int like(char* regexp, char* text){
    fprintf(stderr, "like func with regexp=%s and text=%s\n", regexp, text);
    if (regexp[0] == '\0')
        return 1;
    if (regexp[0] == '%')
        return like_star(regexp+1, text);
    if (*text!='\0' && (regexp[0]=='_' || regexp[0]==*text))
        return like(regexp+1, text+1);
    return 0;
}

int like_star(char* regexp, char* text){
    char* iter = text;
    do {
        if (like(regexp, iter))
            return 1;
    } while (*iter++ != '\0');
    fprintf(stderr, "star failed with *iter=%c and regexp=%s\n", *iter, regexp);
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
        //op[i] = str[i];
        op[i] = str[*(header_point)-op_length+i];
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
    if (next_op == '&' || next_op == '|'){
        val1 = parse_where_recursive(str, header_point, len, tableObj1, tableObj2, idx1, idx2);
    }
    else if (next_op == '<' || next_op == '>' || next_op == '=' || next_op == '*' || next_op == '!'){
        val1 = parse_primary(str, header_point, len, tableObj1, tableObj2, idx1, idx2);
    }
    else{
        fprintf(stderr, "wrong type token in %s\n", str);
        fprintf(stderr, "hedear pointing at %d\n", *header_point);
        return -1;
    }

    // run second evaluation
    // check next op
    next_op = str[*header_point];
    if (next_op == '&' || next_op == '|'){
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


void set_unit_op(char* str, robj* tableObj, int idx){
    int col_idx, col_idx2;
    int header = 0;
    int start_header;
    char* str1;
    char* str2;
    char op;
    long long val1, val2;
    long long retval;
    int n_digits;
    int i;
    //char** row = (char ***) tableObj + idx;
    char** row = tableObj->table[idx];
    for(i=0; i<tableObj->column_length; i++){
        fprintf(stderr, "row[%d] = %s ", i, row[i]);
    }
    fprintf(stderr, "\n");
    // get target column idx
    while(str[header] != '#' && str[header] != '\"'){
        header++;
    }
    str1 = (char*) calloc(1, header + 1);
    memcpy(str1, str, header);
    col_idx = get_col_idx(str1, tableObj->column, tableObj->column_length);
    free(str1);
    fprintf("col %s with idx %d\n", str1, col_idx);
    if (str[header++] == '\"'){
        // assign rvalue
        fprintf(stderr, "assign just the rval\n");
        start_header = header;
        while(str[header] != '\0'){
            header++;
        }
        str1 = (char*) calloc(1, header - start_header + 1);
        memcpy(str1, str+start_header, header - start_header);
        fprintf(stderr, "rval: %s\n", str1);
        free(row[col_idx]);
        row[col_idx] = str1;
        fprintf(stderr, "set_unit_op ended\n");
        return;
    }
    else{
        // get first operend
        start_header = header;
        while(str[header] != '+' && str[header] != '*' && str[header] != '-'){
            header++;
        }
        str1 = (char*) calloc(1, header - start_header + 1);
        memcpy(str1, str+start_header, header - start_header);
        fprintf(stderr, "first operend %s\n", str1);
        // get operator
        op = str[header++];
        fprintf(stderr, "operator: %c\n", op);
        // get second operend
        start_header = header;
        fprintf(stderr, "operend2 start %c\n", str[header]);
        while(str[header] != '\0'){
            fprintf(stderr, "operend2 while  %c\n", str[header]);
            header++;
        }
        fprintf(stderr, "header=%d, start_header=%d\n", header, start_header);
        str2 = (char*) calloc(1, header - start_header + 1);
        memcpy(str2, &str[start_header], header - start_header);
        /*
        for(i=start_header; i<header; i++){
            str2[i-start_header] = str[i];
        }
        */
        fprintf(stderr, "second operend %s\n", str2);
        // check whether they are columns
        col_idx2 = get_col_idx(str1, tableObj->column, tableObj->column_length);
        if (col_idx2 != -1){
            fprintf(stderr, "row[%d] = %s\n", col_idx2, row[col_idx2]);
            val1 = strtoll(row[col_idx2], NULL, 10);
        }
        else
            val1 = strtoll(str1, NULL, 10);
        col_idx2 = get_col_idx(str2, tableObj->column, tableObj->column_length);
        if (col_idx2 != -1)
            val2 = strtoll(row[col_idx2], NULL, 10);
        else
            val2 = strtoll(str2, NULL, 10);
        free(str1);
        free(str2);
        // obtain result
        fprintf(stderr, "val1 = %lld, val2 = %lld\n", val1, val2);
        switch(op){
            case '+':
                val1 += val2;
                break;
            case '-':
                val1 -= val2;
                break;
            case '*':
                val1 *= val2;
                break;
            default:
                fprintf(stderr, "unknown operator %c\n", op);
                return;
        }
        // save the result
        retval = val1;
        fprintf(stderr, "retval = %lld\n", retval);
        n_digits = (int)((ceil(log10(retval))+1)*sizeof(char));
        str1 = (char*) calloc(1, n_digits);
        sprintf(str1, "%lld", retval);
        free(row[col_idx]);
        row[col_idx] = str1;
        return;
    }
}

void parse_set(char* str, robj* tableObj, int idx, int start_header){
    char* unit_op;
    int header = start_header;
    fprintf(stderr, "parse_set init\n");
    while (str[header] != '\r' && str[header] != '\0'){
        header++;
    }
    if (start_header == header){
        return;
    }
    unit_op = (char*) calloc(1, header - start_header + 1);
    memcpy(unit_op, str+start_header, header-start_header);
    set_unit_op(unit_op, tableObj, idx);
    free(unit_op);

    if (str[header] == '\0'){
        fprintf(stderr, "escape from parse_set\n");
        return;
    }
    else{
        header++;
        parse_set(str, tableObj, idx, header);
    }

}
