#ifndef DB_HEADER_FILE
#define DB_HEADER_FILE

/********************************************************************
db.h - This file contains all the structures, defines, and function
prototype for the db.exe program.
*********************************************************************/

#include <stdio.h>

#define MAX_IDENT_LEN 16
#define MAX_STRING_LEN 255
#define MAX_NUM_COL 16
#define MAX_NUM_ROW 1000
#define MAX_NUM_CONDITION 2
#define MAX_TOK_LEN 256
#define KEYWORD_OFFSET 10
#define STRING_BREAK " (),<>="
#define NUMBER_BREAK " ),"

/* Column descriptor sturcture = 20+4+4+4+4 = 36 bytes */
typedef struct cd_entry_def {
	char col_name[MAX_IDENT_LEN+4];
	int col_id; /* Start from 0 */
	int	col_type;
	int	col_len;
	int not_null;
} cd_entry;

/* Table packed descriptor sturcture = 4+20+4+4+4 = 36 bytes
Minimum of 1 column in a table - therefore minimum size of
1 valid tpd_entry is 36+36 = 72 bytes. */
typedef struct tpd_entry_def {
	int tpd_size;
	char table_name[MAX_IDENT_LEN+4];
	int num_columns;
	int cd_offset;
	int tpd_flags;
} tpd_entry;

/* Table packed descriptor list = 4+4+4+36 = 48 bytes.  When no
table is defined the tpd_list is 48 bytes.  When there is 
at least 1 table, then the tpd_entry (36 bytes) will be
overlapped by the first valid tpd_entry. */
typedef struct tpd_list_def {
	int	list_size;
	int	num_tables;
	int	db_flags;
	tpd_entry tpd_start;
} tpd_list;

/* This token_list definition is used for breaking the command
string into separate tokens in function get_tokens().  For
each token, a new token_list will be allocated and linked 
together. */
typedef struct token_list_def {
	char tok_string[MAX_TOK_LEN];
	int tok_class;
	int tok_value;
	struct token_list_def *next;
} token_list;

/* This enum defines the different classes of tokens for 
semantic processing. */
typedef enum token_class_def {
	TOKEN_CLASS_KEYWORD = 1, // 1
	TOKEN_CLASS_IDENTIFIER, // 2
	TOKEN_CLASS_SYMBOL, // 3
	TOKEN_CLASS_TYPE_NAME, // 4
	TOKEN_CLASS_CONSTANT, // 5
	TOKEN_CLASS_FUNCTION_NAME, // 6
	TOKEN_CLASS_TERMINATOR, // 7
	TOKEN_CLASS_ERROR // 8
} token_class;

/* This enum defines the different values associated with
a single valid token.  Use for semantic processing. */
typedef enum token_value_def {
	T_INT = 10, // 10 - new type should be added above this line
	T_CHAR, // 11 
	K_CREATE, // 12
	K_TABLE, // 13
	K_NOT, // 14
	K_NULL, // 15
	K_DROP, // 16
	K_LIST, // 17
	K_SCHEMA, // 18
	K_FOR, // 19
	K_TO, // 20
	K_INSERT, // 21
	K_INTO, // 22
	K_VALUES, // 23
	K_DELETE, // 24
	K_FROM, // 25
	K_WHERE, // 26
	K_UPDATE, // 27
	K_SET, // 28
	K_SELECT, // 29
	K_ORDER, // 30
	K_BY, // 31
	K_DESC, // 32
	K_IS, // 33
	K_AND, // 34
	K_OR, // 35 - new keyword should be added below this line
	F_SUM, // 36
	F_AVG, // 37
	F_COUNT, // 38 - new function name should be added below this line
	S_LEFT_PAREN = 70,  // 70
	S_RIGHT_PAREN, // 71
	S_COMMA, // 72
	S_STAR, // 73
	S_EQUAL, // 74
	S_LESS, // 75
	S_GREATER, // 76
	IDENT = 85, // 85
	INT_LITERAL = 90, // 90
	STRING_LITERAL, // 91
	EOC = 95, // 95
	INVALID = 99 // 99
} token_value;

/* This constants must be updated when add new keywords */
#define TOTAL_KEYWORDS_PLUS_TYPE_NAMES 29

/* New keyword must be added in the same position/order as the enum
definition above, otherwise the lookup will be wrong */
const char * const keyword_table[] = {
	"int", "char", "create", "table", "not", "null", "drop", "list", "schema",
	"for", "to", "insert", "into", "values", "delete", "from", "where", 
	"update", "set", "select", "order", "by", "desc", "is", "and", "or",
	"sum", "avg", "count"
};

/* This enum defines a set of possible statements */
typedef enum semantic_statement_def {
	INVALID_STATEMENT = -199, // -199
	CREATE_TABLE = 100, // 100
	DROP_TABLE, // 101
	LIST_TABLE, // 102
	LIST_SCHEMA, // 103
	INSERT, // 104
	DELETE, // 105
	UPDATE, // 106
	SELECT // 107
} semantic_statement;

/* This enum has a list of all the errors that should be detected
by the program.  Can append to this if necessary. */
typedef enum return_codes_def {
	INVALID_TABLE_NAME = -399, // -399
	DUPLICATE_TABLE_NAME, // -398
	TABLE_NOT_EXIST, // -397
	INVALID_TABLE_DEFINITION, // -396
	INVALID_COLUMN_NAME, // -395
	DUPLICATE_COLUMN_NAME, // -394
	COLUMN_NOT_EXIST, // -393
	MAX_COLUMN_EXCEEDED, // -392
	INVALID_TYPE_NAME, // -391
	INVALID_COLUMN_DEFINITION, // -390
	INVALID_COLUMN_LENGTH, // -389
	INVALID_REPORT_FILE_NAME, // -388
	INVALID_VALUE, // -387
	INVALID_VALUES_COUNT, // -386
	INVALID_AGGREGATE_COLUMN, // -385
	INVALID_CONDITION, // -384
	INVALID_CONDITION_OPERAND, // -383
	MAX_ROW_EXCEEDED, // -382
	DATA_TYPE_MISMATCH, // -381
	UNEXPECTED_NULL_VALUE, // 380
	/* Must add all the possible errors from I/U/D + SELECT here */
	FILE_OPEN_ERROR = -299, // -299
	DBFILE_CORRUPTION, // -298
	MEMORY_ERROR, // -297
	FILE_REMOVE_ERROR, // -296
	TABFILE_CORRUPTION // -295
} return_codes;

/* Table file structures in which we store records of that table */
typedef struct table_file_header_def {
	int file_size;
	int record_size;
	int num_records;
	int record_offset;
	int file_header_flag;
	tpd_entry *tpd_ptr;
} table_file_header;

enum class FieldValueType {UNKNOWN, INT, STRING};

/* The structure to represent the int/string typed value in each record field. */
typedef struct field_value_def {
	FieldValueType type;
	bool is_null;
	int int_value; // Fill int value here.
	char string_value[MAX_STRING_LEN + 1]; // Fill string value here.
	token_list *linked_token; // Point to the original token.
	int col_id; // Column ID.
} field_value;

/* The structure to represent the field value of each record field. */
typedef struct field_name_def {
	char name[MAX_IDENT_LEN + 1]; // Fill field name here.
	token_list *linked_token; // Point to the original token.
} field_name;

/* One row as a record of DB */
typedef struct record_row_def {
	int num_fields;
	field_value *value_ptrs[MAX_NUM_COL];
	int sorting_col_id;
	struct record_row_def *next;
} record_row;

/* Condition in record-level predicate by WHERE clause. */
typedef struct record_condition_def {
	FieldValueType value_type; // Type of the field. It is available only the operator is in {S_LESS, S_GREATER, S_EQUAL}.
	int col_id; // LHS operand.
	int op_type; // Relational operator, can be S_LESS, S_GREATER, S_EQUAL, K_IS (used in "IS NULL"), or K_NOT (used in "IS NOT NULL").
	int int_data_value; // RHS operand. It is available only if data type is integer and operator is in {S_LESS, S_GREATER, S_EQUAL}.
	char string_data_value[MAX_STRING_LEN+1]; // RHS operand. It is available only if data type is string and operator is in {S_LESS, S_GREATER, S_EQUAL}.
} record_condition;

/* Record predicate represented as WHERE clause. */
typedef struct record_predicate_def {
	int type; // The relationship of conditions, can be K_AND or K_OR. Set as K_AND by default if there is only one condition.
	int num_conditions;
	record_condition conditions[MAX_NUM_CONDITION];
} record_predicate;

/* Set of function prototypes */
int get_token(char *command, token_list **tok_list);
void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value);
int do_semantic(token_list *tok_list);
int sem_create_table(token_list *t_list);
int sem_drop_table(token_list *t_list);
int sem_list_tables();
int sem_list_schema(token_list *t_list);
int sem_insert(token_list *t_list);
int sem_select(token_list *t_list);
int sem_delete(token_list *t_list);
int sem_update(token_list *t_list);
int initialize_tpd_list();
int add_tpd_to_list(tpd_entry *tpd);
int drop_tpd_from_list(char *tabname);
tpd_entry* get_tpd_from_list(char *tabname);
int create_tab_file(char* table_name, cd_entry cd_entries[], int num_columns);
int check_insert_values(field_value field_values[], int num_values, cd_entry cd_entries[], int num_columns);
void free_token_list(token_list* const t_list);
int load_table_records(tpd_entry *tpd, table_file_header **pp_table_header);
int get_file_size(FILE *fhandle);
int fill_raw_record_bytes(cd_entry cd_entries[], field_value *field_values[], int num_cols, char record_bytes[], int num_record_bytes);
int fill_record_row(cd_entry cd_entries[], int num_cols, record_row *p_row, char record_bytes[]);
void print_table_border(cd_entry *sorted_cd_entries[], int num_values);
void print_table_column_names(cd_entry *sorted_cd_entries[], field_name field_names[], int num_values);
void print_record_row(cd_entry *sorted_cd_entries[], int num_cols, record_row *row);
void print_aggregate_result(int aggregate_type, int num_fields, int records_count, int int_sum, cd_entry *sorted_cd_entries[]);
int column_display_width(cd_entry *col_entry);
int get_cd_entry_index(cd_entry cd_entries[], int num_cols, char *col_name);
bool apply_row_predicate(cd_entry cd_entries[], int num_cols, record_row *p_row, record_predicate *p_predicate);
bool eval_condition(record_condition *p_condition, field_value *p_field_value);
void sort_records(record_row rows[], int num_records, cd_entry *p_sorting_col, bool is_desc);
int execute_statement(char *statement);
int records_comparator(const void *arg1, const void *arg2);
void free_record_row(record_row *row, bool to_last);
int save_records_to_file(table_file_header * const tab_header, record_row * const rows_head);
int reload_global_tpd_list();


/* inline functions */

/* Get column descriptor entries from the table descriptor entry. */
inline void get_cd_entries(tpd_entry *tab_entry, cd_entry **pp_cd_entry) {
	*pp_cd_entry = (cd_entry *) (((char *) tab_entry) + tab_entry->cd_offset);
}

/* Get pointer to the first record in a table. */
inline void get_table_records(table_file_header *tab_header, char **pp_record) {
	*pp_record = NULL;
	if (tab_header->file_size > tab_header->record_offset) {
		*pp_record = ((char *) tab_header) + tab_header->record_offset;
	}
}

/* Check if a token can be an identifier. */
inline bool can_be_identifier(token_list *token) {
	if (!token) {
		return false;
	}
	// Any keyword and type name can also be a valid identifier.
	return (token->tok_class == TOKEN_CLASS_KEYWORD) ||
		(token->tok_class == TOKEN_CLASS_IDENTIFIER) ||
		(token->tok_class == TOKEN_CLASS_TYPE_NAME);
}

/* Integer round */
inline int round_integer(int value, int round) {
	int m = value % round;
	return m ? (value + round - m) : value;
}

inline void repeat_print_char(char c, int times) {
	for (int i = 0; i < times; i++) {
		printf("%c", c);
	}
}


/* Keep a global list of tpd which will be initialized in db.cpp */
extern tpd_list *g_tpd_list;

#endif /* DB_HEADER_FILE */
