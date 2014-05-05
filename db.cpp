/************************************************************
Project#1:	CLP & DDL
************************************************************/

#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#endif

#include "db.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <search.h>
#include <ctype.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>

/*
Keep a global list of tpd - in real life, this will be stored
in shared memory.  Build a set of functions/methods around this.
*/
tpd_list *g_tpd_list = NULL;

int main(int argc, char** argv) {
	if ((argc != 2) || (strlen(argv[1]) == 0)) {
		printf("Usage: db \"command statement\"");
		return 1;
	}

	int rc = execute_statement(argv[1]);

	// Free g_tpd_list since all changes have been stored in files.
	free(g_tpd_list);

	return rc;
}

int reload_global_tpd_list() {
	free(g_tpd_list);
	return initialize_tpd_list();
}

int execute_statement(char *statement) {
	token_list *tok_list = NULL, *tok_ptr = NULL;
	int rc = initialize_tpd_list();

	if (rc) {
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
	} else {
		rc = get_token(statement, &tok_list);

		// Show tokens for test purpose.
		tok_ptr = tok_list;
		printf("%16s %10s %10s\n", "Tokens: STRING", "CLASS", "VALUE");
		repeat_print_char('-', 16 + 11 + 11);
		printf("\n");
		while (tok_ptr != NULL) {
			printf("%16s %10d %10d\n", tok_ptr->tok_string, tok_ptr->tok_class, tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}
		repeat_print_char('-', 16 + 11 + 11);
		printf("\n\n");

		int cmd_type = INVALID_STATEMENT;
		if (!rc) {
			rc = do_semantic(tok_list, &cmd_type);

			// Log command if it is executed successfully.
			if (!rc) {
				switch(cmd_type) {
				case CREATE_TABLE:
				case DROP_TABLE:
				case INSERT:
				case DELETE:
				case UPDATE:
					// Log '<timestamp> "original DDL/DML statement within double quotes"'
					write_log_with_timestamp(statement, current_timestamp());
					break;
				case BACKUP_TO_IMAGE:
				case RESTORE_FROM_IMAGE:
					// Log 'BACKUP <image file name>' or 'RF_START' if necessary.
					break;
				}
			}
		}

		if (rc) {
			tok_ptr = tok_list;
			while (tok_ptr != NULL) {
				if ((tok_ptr->tok_class == TOKEN_CLASS_ERROR) || (tok_ptr->tok_value == INVALID)) {
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}

		// Whether the token list is valid or not, we need to free the memory.
		free_token_list(tok_list);
	}

	return rc;
}

/************************************************************* 
This is a lexical analyzer for simple SQL statements
*************************************************************/
int get_token(char* command, token_list** tok_list) {
	int rc = 0, i, j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;

	start = cur = command;
	while (!done) {
		bool found_keyword = false;

		/* This is the TOP Level for each token */
		memset ((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ') {
			cur++;
		}

		if (cur && isalpha(*cur)) {
			// find valid identifier
			int t_class;
			do {
				temp_string[i++] = *cur++;
			} while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur))) {
				/* If the next char following the keyword or identifier
				is not a blank, (, ), or a comma, then append this
				character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, TOKEN_CLASS_ERROR, INVALID);
				rc = INVALID;
				done = true;
			} else {
				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++) {
					if ((stricmp(keyword_table[j], temp_string) == 0)) {
						found_keyword = true;
						break;
					}
				}

				if (found_keyword) {
					if (KEYWORD_OFFSET+j < K_CREATE) {
						t_class = TOKEN_CLASS_TYPE_NAME;
					} else if (KEYWORD_OFFSET+j >= F_SUM) {
						t_class = TOKEN_CLASS_FUNCTION_NAME;
					} else {
						t_class = TOKEN_CLASS_KEYWORD;
					}

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
				} else {
					if (strlen(temp_string) <= MAX_IDENT_LEN) {
						add_to_list(tok_list, temp_string, TOKEN_CLASS_IDENTIFIER, IDENT);
					} else {
						add_to_list(tok_list, temp_string, TOKEN_CLASS_ERROR, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur) {
					add_to_list(tok_list, "", TOKEN_CLASS_TERMINATOR, EOC);
					done = true;
				}
			}
		} else if (isdigit(*cur)) {
			// find valid number
			do {
				temp_string[i++] = *cur++;
			} while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur))) {
				/* If the next char following the keyword or identifier
				is not a blank or a ), then append this
				character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, TOKEN_CLASS_ERROR, INVALID);
				rc = INVALID;
				done = true;
			} else {
				add_to_list(tok_list, temp_string, TOKEN_CLASS_CONSTANT, INT_LITERAL);

				if (!*cur) {
					add_to_list(tok_list, "", TOKEN_CLASS_TERMINATOR, EOC);
					done = true;
				}
			}
		} else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
			|| (*cur == '=') || (*cur == '<') || (*cur == '>')) {
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur) {
			case '(':
				t_value = S_LEFT_PAREN;
				break;
			case ')':
				t_value = S_RIGHT_PAREN;
				break;
			case ',':
				t_value = S_COMMA;
				break;
			case '*':
				t_value = S_STAR;
				break;
			case '=':
				t_value = S_EQUAL;
				break;
			case '<':
				t_value = S_LESS;
				break;
			case '>':
				t_value = S_GREATER;
				break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, TOKEN_CLASS_SYMBOL, t_value);

			if (!*cur) {
				add_to_list(tok_list, "", TOKEN_CLASS_TERMINATOR, EOC);
				done = true;
			}
		} else if (*cur == '\'') {
			/* Find STRING_LITERRAL */
			cur++;
			do {
				temp_string[i++] = *cur++;
			} while ((*cur) && (*cur != '\''));

			temp_string[i] = '\0';

			if (!*cur) {
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, TOKEN_CLASS_ERROR, INVALID);
				rc = INVALID;
				done = true;
			} else {
				/* must be a ' */
				add_to_list(tok_list, temp_string, TOKEN_CLASS_CONSTANT, STRING_LITERAL);
				cur++;
				if (!*cur) {
					add_to_list(tok_list, "", TOKEN_CLASS_TERMINATOR, EOC);
					done = true;
				}
			}
		} else {
			if (!*cur) {
				add_to_list(tok_list, "", TOKEN_CLASS_TERMINATOR, EOC);
				done = true;
			} else {
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, TOKEN_CLASS_ERROR, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}

	return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value) {
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

	if (cur == NULL) {
		*tok_list = ptr;
	} else {
		while (cur->next != NULL) {
			cur = cur->next;
		}
		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list, int *p_cmd_type)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
	token_list *cur = tok_list;

	if ((cur->tok_value == K_CREATE) &&
		((cur->next != NULL) && (cur->next->tok_value == K_TABLE))) {
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	} else if ((cur->tok_value == K_DROP) &&
		((cur->next != NULL) && (cur->next->tok_value == K_TABLE))) {
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	} else if ((cur->tok_value == K_LIST) &&
		((cur->next != NULL) && (cur->next->tok_value == K_TABLE))) {
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	} else if ((cur->tok_value == K_LIST) &&
		((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA))) {
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	} else if ((cur->tok_value == K_INSERT) &&
		((cur->next != NULL) && (cur->next->tok_value == K_INTO))) {
		printf("INSERT statement\n");
		cur_cmd = INSERT;
		cur = cur->next->next;
	} else if ((cur->tok_value == K_DELETE) &&
		((cur->next != NULL) && (cur->next->tok_value == K_FROM))) {
		printf("DELETE statement\n");
		cur_cmd = DELETE;
		cur = cur->next->next;
	} else if (cur->tok_value == K_UPDATE) {
		printf("UPDATE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;
	} else if (cur->tok_value == K_SELECT) {
		printf("SELECT statement\n");
		cur_cmd = SELECT;
		cur = cur->next;
	} else if ((cur->tok_value == K_BACKUP) &&
		((cur->next != NULL) && (cur->next->tok_value == K_TO))) {
		printf("BACKUP TO statement\n");
		cur_cmd = BACKUP_TO_IMAGE;
		cur = cur->next->next;
	} else if ((cur->tok_value == K_RESTORE) &&
		((cur->next != NULL) && (cur->next->tok_value == K_FROM))) {
		printf("RESTORE FROM statement\n");
		cur_cmd = RESTORE_FROM_IMAGE;
		cur = cur->next->next;
	} else if (cur->tok_value == K_ROLLFORWARD) {
		printf("ROLLFORWARD statement\n");
		cur_cmd = ROLLFORWARD;
		cur = cur->next;
	} else {
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd != INVALID_STATEMENT) {
		switch (cur_cmd) {
		case CREATE_TABLE:
			rc = sem_create_table(cur);
			break;
		case DROP_TABLE:
			rc = sem_drop_table(cur);
			break;
		case LIST_TABLE:
			rc = sem_list_tables();
			break;
		case LIST_SCHEMA:
			rc = sem_list_schema(cur);
			break;
		case INSERT:
			rc = sem_insert(cur);
			break;
		case DELETE:
			rc = sem_delete(cur);
			break;
		case UPDATE:
			rc = sem_update(cur);
			break;
		case SELECT:
			rc = sem_select(cur);
			break;
		default:
			; /* no action */
		}
	}

	*p_cmd_type = cur_cmd;
	return rc;
}

int sem_create_table(token_list *t_list) {
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry col_entry[MAX_NUM_COL];


	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;
	if (!can_be_identifier(cur)) {
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	} else {
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL) {
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		} else {
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN) {
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			} else {
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do {
					// Check if columns count is greater than MAX_NUM_COL.
					if (cur_id >= MAX_NUM_COL) {
						rc = MAX_COLUMN_EXCEEDED;
						cur->tok_value = INVALID;
						return rc;
					}

					if (!can_be_identifier(cur)) {
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					} else {
						int i;
						for (i = 0; i < cur_id; i++) {
							/* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string) == 0) {
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc) {
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != TOKEN_CLASS_TYPE_NAME) {
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							} else {
								/* Set the column type here, T_INT or T_CHAR */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;

								// If current column type is T_INT.
								if (col_entry[cur_id].col_type == T_INT) {
									if ((cur->tok_value != S_COMMA) &&
										(cur->tok_value != K_NOT) &&
										(cur->tok_value != S_RIGHT_PAREN)) {
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									} else {
										col_entry[cur_id].col_len = sizeof(int);

										if ((cur->tok_value == K_NOT) &&
											(cur->next->tok_value != K_NULL)) {
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}	
										else if ((cur->tok_value == K_NOT) &&
											(cur->next->tok_value == K_NULL))
										{					
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}

										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												(cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												if (cur->tok_value == S_RIGHT_PAREN)
												{
													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of T_INT processing
								else
								{
									// It must be char()
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;

										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											cur = cur->next;

											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;

												if ((cur->tok_value != S_COMMA) &&
													(cur->tok_value != K_NOT) &&
													(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														(cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
														(cur->next->tok_value == K_NULL))
													{					
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}

													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) &&															  (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
														{
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));

				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + 
						sizeof(cd_entry) *	tab_entry.num_columns;
					tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry,
							(void*)&tab_entry,
							sizeof(tpd_entry));

						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
							(void*)col_entry,
							sizeof(cd_entry) * tab_entry.num_columns);

						rc = add_tpd_to_list(new_entry);

						// Create .tab file.
						if (!rc) {
							rc = create_tab_file(tab_entry.table_name, col_entry, tab_entry.num_columns);
							if (rc) {
								cur->tok_value = INVALID;
							}
						}

						free(new_entry);
					}
				}
			}
		}
	}
	return rc;
}

int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if (!can_be_identifier(cur)) {
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	} else {
		if (cur->next->tok_value != EOC) {
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		} else {
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL) {
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			} else {
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);
				if (!rc) {
					// Also delete table_name.tab file.
					char table_filename[MAX_IDENT_LEN + 5];
					sprintf(table_filename, "%s.tab", cur->tok_string);
					if (remove(table_filename) != 0) {
						rc = FILE_REMOVE_ERROR;
						cur->tok_value = INVALID;
					}
				}
			}
		}
	}

	return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

	return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN + 1];
	char filename[MAX_IDENT_LEN + 1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if (!can_be_identifier(cur))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;

					if (!can_be_identifier(cur))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{ 
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if ((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
						printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
							fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 
						}

						/* Next, write the cd_entry information */
						get_cd_entries(tab_entry, &col_entry);
						for (i = 0; i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}

						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

	return rc;
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;

	/* Open for read */
	if ((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		// DB file "dbfile.bin" doesn't exist, so create it.
		if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));

			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
		int file_size = get_file_size(fhandle);
		printf("dbfile.bin size = %ld\n", file_size);
		g_tpd_list = (tpd_list*)calloc(1, file_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_size, 1, fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_size)
			{
				rc = DBFILE_CORRUPTION;
			}

		}
	}

	return rc;
}

int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
			g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
			g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
				else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								old_size - cur->tpd_size -
								(sizeof(tpd_list) - sizeof(tpd_entry)),
								1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
						g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
							1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						has already subtracted the cur->tpd_size, therefore it will
						point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size, old_size - cur->tpd_size - ((char*)cur - (char*)g_tpd_list), 1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}
			}
			else
			{
				/* not found yet, go on scanning next tpd */
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}

	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}

int sem_insert(token_list *t_list) {
	int rc = 0;
	token_list *cur = t_list;

	if (!can_be_identifier(cur)) {
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
			return rc;
	}

	// Check whether the table name exists.
	tpd_entry *tab_entry = get_tpd_from_list(cur->tok_string);

	if (tab_entry == NULL) {
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
		return rc;
	}

	cur = cur->next;
	if ((cur->tok_value != K_VALUES) || (cur->next->tok_value != S_LEFT_PAREN))	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	cur = cur->next->next;

	// Read all the value tokens and store them in a field_value array.
	bool values_done = false;
	field_value field_values[MAX_NUM_COL];
	memset(field_values, '\0', sizeof(field_values));
	int num_values = 0;
	while (!values_done) {
		// Check if values count is greater than MAX_NUM_COL.
		if (num_values >= MAX_NUM_COL) {
			rc = MAX_COLUMN_EXCEEDED;
			cur->tok_value = INVALID;
			return rc;
		}

		if (cur->tok_value == STRING_LITERAL) {
			field_values[num_values].type = FIELD_VALUE_TYPE_STRING;
			field_values[num_values].is_null = false;
			strcpy(field_values[num_values].string_value, cur->tok_string);
			field_values[num_values].linked_token = cur;
			field_values[num_values].col_id = num_values;
		} else if (cur->tok_value == INT_LITERAL) {
			field_values[num_values].type = FIELD_VALUE_TYPE_INT;
			field_values[num_values].is_null = false;
			field_values[num_values].int_value = atoi(cur->tok_string);
			field_values[num_values].linked_token = cur;
			field_values[num_values].col_id = num_values;
		} else if (cur->tok_value == K_NULL) {
			field_values[num_values].type = FIELD_VALUE_TYPE_UNKNOWN;
			field_values[num_values].is_null = true;
			field_values[num_values].linked_token = cur;
			field_values[num_values].col_id = num_values;
		} else {
			rc = INVALID_VALUE;
			break;
		}
		cur = cur->next;
		if (cur->tok_value == S_COMMA) {
			// nothing to do
		} else if (cur->tok_value == S_RIGHT_PAREN) {
			values_done = true;
		} else {
			rc = INVALID_STATEMENT;
			break;
		}
		cur = cur->next;
		num_values++;
	}

	if (values_done && (cur->tok_value != EOC)) {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	if (rc) {
		cur->tok_value = INVALID;
		return rc;
	}

	cd_entry *cd_entries = NULL;
	get_cd_entries(tab_entry, &cd_entries);
	// The order and number of columns must be same for field_values and cd_entries.
	rc = check_insert_values(field_values, num_values, cd_entries, tab_entry->num_columns);

	if (rc) {
		cur->tok_value = INVALID;
		return rc;
	}

	
	// It is the heap memory owner of the content of the whole table.
	// Do not forget to free it later.
	table_file_header *tab_header = NULL;
	if ((rc = load_table_records(tab_entry, &tab_header)) != 0) {
		return rc;
	}

	if (tab_header->num_records >= MAX_NUM_ROW) {
		rc = MAX_ROW_EXCEEDED;
		free(tab_header);
		return rc;
	}

	// Compose the new record.
	char *record_bytes = (char *) malloc(tab_header->record_size);
	field_value *field_value_ptrs[MAX_NUM_COL];
	for (int i = 0; i < num_values; i++) {
		field_value_ptrs[i] = &field_values[i];
	}
	fill_raw_record_bytes(cd_entries, field_value_ptrs, num_values, record_bytes, sizeof(tab_header->record_size));

	// Append the new record to the .tab file.
	char table_filename[MAX_IDENT_LEN + 5];
	sprintf(table_filename, "%s.tab", tab_header->tpd_ptr->table_name);
	FILE *fhandle = NULL;
	if ((fhandle = fopen(table_filename, "wbc")) == NULL) {
		rc = FILE_OPEN_ERROR;
	} else {
		// Add one more record in table header.
		int old_table_file_size = tab_header->file_size;
		tab_header->num_records++;
		tab_header->file_size += tab_header->record_size;
		tab_header->tpd_ptr = NULL; // Reset tpd pointer.

		fwrite(tab_header, old_table_file_size, 1, fhandle);
		fwrite(record_bytes, tab_header->record_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}
	free(record_bytes);
	free(tab_header);
	return rc;
}

int sem_select(token_list *t_list) {
	int rc = 0;
	token_list *cur = t_list;

	field_name field_names[MAX_NUM_COL];
	bool fields_done = false;
	int wildcard_field_index = -1;
	int num_fields = 0;

	// Try parse aggregate operator: SUM, AVG, or COUNT.
	int aggregate_type = 0;
	if (cur->tok_class == TOKEN_CLASS_FUNCTION_NAME && cur->next->tok_value == S_LEFT_PAREN) {
		aggregate_type = cur->tok_value; // Can be F_COUNT, F_SUM, or F_AVG.
		if (!(can_be_identifier(cur->next->next) || (cur->next->next != NULL && cur->next->next->tok_value == S_STAR))) {
			// Error column name.
			rc = INVALID_COLUMN_NAME;
			cur->tok_value = INVALID;
			return rc;
		}
		cur = cur->next->next;
		strcpy(field_names[num_fields].name, cur->tok_string);
		field_names[num_fields].linked_token = cur;
		if (strcmp(cur->tok_string, "*") == 0) {
			wildcard_field_index = 0;
		}
		cur = cur->next;
		if (cur->tok_value != S_RIGHT_PAREN) {
			// Error column name.
			rc = INVALID_COLUMN_NAME;
			cur->tok_value = INVALID;
			return rc;
		} else {
			cur = cur->next;
			num_fields++;
			fields_done = true;
		}
	}
	
	// Extract field names. However, it will be skipped if the unique aggregate field is found.
	while (!fields_done) {
		if (!(can_be_identifier(cur) || cur->tok_value == S_STAR)) {
				// Error column name.
				rc = INVALID_COLUMN_NAME;
				cur->tok_value = INVALID;
				return rc;
		}

		// Check if wildcard has already been caught.
		if (wildcard_field_index == 0) {
			rc = INVALID_COLUMN_NAME;
			cur->tok_value = INVALID;
			return rc;
		}

		if (strcmp(cur->tok_string, "*") == 0) {
			// Wildcard must appear only once, as the first column name.
			if (wildcard_field_index == -1 && num_fields == 0) {
				wildcard_field_index = 0;
			} else {
				rc = INVALID_COLUMN_NAME;
				cur->tok_value = INVALID;
				return rc;
			}
		}
		strcpy(field_names[num_fields].name, cur->tok_string);
		field_names[num_fields].linked_token = cur;
		num_fields++;
		cur = cur->next;
		if (cur->tok_value == S_COMMA) {
			cur = cur->next;
		} else {
			fields_done = true;
		}
	}
	
	if (cur->tok_value != K_FROM) {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	cur = cur->next;
	if (!can_be_identifier(cur)) {
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
		return rc;
	}

	// Check whether the table name exists.
	tpd_entry *tab_entry = get_tpd_from_list(cur->tok_string);
	if (tab_entry == NULL) {
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
		return rc;
	}

	// Get column descriptors.
	cd_entry *cd_entries = NULL;
	get_cd_entries(tab_entry, &cd_entries);

	// Expand wildcard field to all field names of the table.
	if (wildcard_field_index == 0) {
		token_list *wildcard_token = field_names[wildcard_field_index].linked_token;
		num_fields = tab_entry->num_columns;
		for (int i = 0; i < num_fields; i++) {
			strcpy(field_names[i].name, cd_entries[i].col_name);
			field_names[i].linked_token = wildcard_token;
		}
	}

	// Check if all field names exist in that table and generate sorted_cd_entries.
	cd_entry *sorted_cd_entries[MAX_NUM_COL];
	for (int i = 0; i < num_fields; i++) {
		int col_index = get_cd_entry_index(cd_entries, tab_entry->num_columns, field_names[i].name);
		if (col_index < 0) {
			rc = INVALID_COLUMN_NAME;
			field_names[i].linked_token->tok_value = INVALID;
			return rc;
		} else {
			// SUM and AVG aggregate functions are only valid on the unique interger column.
			if ((aggregate_type == F_SUM) || (aggregate_type == F_AVG)) {
				if ((num_fields == 1) && (cd_entries[col_index].col_type == T_INT)) {
					sorted_cd_entries[i] = &cd_entries[col_index];
				} else {
					rc = INVALID_AGGREGATE_COLUMN;
					field_names[i].linked_token->tok_value = INVALID;
					return rc;
				}
			} else {
				sorted_cd_entries[i] = &cd_entries[col_index];
			}
		}
	}

	bool has_where_clause = false;
	record_predicate row_filter;
	memset(&row_filter, '\0', sizeof(record_predicate));
	row_filter.type = K_AND; // Set default relationship of conditions.

	cur = cur->next;
	// Parse optional WHERE clause.
	if (cur->tok_value == K_WHERE) {
		has_where_clause = true;

		int col_index = -1;
		int num_conditions = 0;
		bool has_more_condition = true;

		while (has_more_condition) {
			// Read column name.
			cur = cur->next;
			if (can_be_identifier(cur)) {
				col_index = get_cd_entry_index(cd_entries, tab_entry->num_columns, cur->tok_string);
				if (col_index > -1) {
					row_filter.conditions[num_conditions].col_id = col_index;
					row_filter.conditions[num_conditions].value_type = ((cd_entries[col_index].col_type == T_INT) ? FIELD_VALUE_TYPE_INT : FIELD_VALUE_TYPE_STRING);
				} else {
					rc = INVALID_COLUMN_NAME;
					cur->tok_value = INVALID;
					return rc;
				}
			} else {
				rc = INVALID_COLUMN_NAME;
				cur->tok_value = INVALID;
				return rc;
			}

			// Read relational operator of first condition.
			cur = cur->next;
			if (cur->tok_value == S_LESS || cur->tok_value == S_GREATER || cur->tok_value == S_EQUAL) {
				row_filter.conditions[num_conditions].op_type = cur->tok_value;
				cur = cur->next;
				if (cur->tok_value == INT_LITERAL) {
					if (row_filter.conditions[num_conditions].value_type == FIELD_VALUE_TYPE_INT) {
						row_filter.conditions[num_conditions].int_data_value = atoi(cur->tok_string);
					} else {
						rc = INVALID_CONDITION_OPERAND;
						cur->tok_value = INVALID;
						return rc;
					}
				} else if (cur->tok_value == STRING_LITERAL) {
					if (row_filter.conditions[num_conditions].value_type == FIELD_VALUE_TYPE_STRING) {
						strcpy(row_filter.conditions[num_conditions].string_data_value, cur->tok_string);
					} else {
						rc = INVALID_CONDITION_OPERAND;
						cur->tok_value = INVALID;
						return rc;
					}
				} else {
					rc = INVALID_CONDITION;
					cur->tok_value = INVALID;
					return rc;
				}
			} else if (cur->tok_value == K_IS && cur->next->tok_value == K_NULL) { // "IS NULL"
				cur = cur->next;
				row_filter.conditions[num_conditions].op_type = K_IS;
			} else if (cur->tok_value == K_IS && cur->next->tok_value == K_NOT && cur->next->next->tok_value == K_NULL) { // "IS NOT NULL"
				cur = cur->next->next;
				row_filter.conditions[num_conditions].op_type = K_NOT;
			} else {
				rc = INVALID_CONDITION;
				cur->tok_value = INVALID;
				return rc;
			}
			num_conditions++;
			row_filter.num_conditions = num_conditions;

			if (num_conditions == MAX_NUM_CONDITION) {
				break;
			}

			if (cur->next->tok_value == K_AND || cur->next->tok_value == K_OR) {
				cur = cur->next;
				has_more_condition = true;
				row_filter.type = cur->tok_value;
			} else {
				has_more_condition = false;
			}
		} // End of while.
		cur = cur->next;
	}
	
	// Parse ORDER BY clause
	bool has_order_by_clause = false;
	bool order_by_desc = false;
	int order_by_column_id = -1;

	if (cur->tok_value == K_ORDER && cur->next->tok_value == K_BY) {
		cur = cur->next->next;
		if (can_be_identifier(cur)) {
			order_by_column_id = get_cd_entry_index(cd_entries, tab_entry->num_columns, cur->tok_string);
			if (order_by_column_id > -1) {
				has_order_by_clause = true;
				cur = cur->next;
				if (cur->tok_value == K_DESC) {
					order_by_desc = true;
					cur = cur->next;
				}
			} else {
				rc = INVALID_COLUMN_NAME;
				cur->tok_value = INVALID;
				return rc;
			}
		} else {
			rc = INVALID_COLUMN_NAME;
			cur->tok_value = INVALID;
			return rc;
		}
	}

	if (cur->tok_value != EOC) {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	// It is the heap memory owner of the content of the whole table.
	// Do not forget to free it later.
	table_file_header *tab_header = NULL;
	if ((rc = load_table_records(tab_entry, &tab_header)) != 0) {
		return rc;
	}

	// Load record rows.
	char *record_in_table = NULL;
	get_table_records(tab_header, &record_in_table);
	int aggregate_int_sum = 0;
	int num_loaded_records = 0;
	int aggregate_records_count = 0;

	record_row record_rows[MAX_NUM_ROW];
	memset(record_rows, '\0', sizeof(record_rows));
	record_row *p_current_row = NULL;
	for (int i = 0; i < tab_header->num_records; i++) {
		// Fill all field values (not only displayed columns) from current record.
		p_current_row = &record_rows[num_loaded_records];
		fill_record_row(cd_entries, tab_entry->num_columns, p_current_row, record_in_table);

		// Do filtering on current record row.
		if (has_where_clause && (!apply_row_predicate(cd_entries, tab_entry->num_columns, p_current_row, &row_filter))) {
			// Current row is not qualified so should be skipped (i.e. will not be loaded in final result set).
			for (int j = 0; j < p_current_row->num_fields; j++) {
				free(p_current_row->value_ptrs[j]);
			}
			memset(p_current_row, '\0', sizeof(record_row));
		} else {
			/* Current row survives. */
			num_loaded_records++;

			if ((aggregate_type == F_SUM) || (aggregate_type == F_AVG)) {
				// SUM(col) or AVG(col), ignore NULL rows.
				if (!p_current_row->value_ptrs[sorted_cd_entries[0]->col_id]->is_null) {
					aggregate_int_sum += p_current_row->value_ptrs[sorted_cd_entries[0]->col_id]->int_value;
					aggregate_records_count++;
				}
			} else if (aggregate_type == F_COUNT) {
				if (num_fields == 1) {
					// count(col), ignore NULL rows.
					if (!p_current_row->value_ptrs[sorted_cd_entries[0]->col_id]->is_null) {
						aggregate_records_count++;
					}
				} else {
					// count(*), include NULL rows.
					aggregate_records_count++;
				}
			}
		}

		// Move forward to next record.
		record_in_table += tab_header->record_size;
	}
	if (aggregate_type == 0) {
		print_table_border(sorted_cd_entries, num_fields);
		print_table_column_names(sorted_cd_entries, field_names, num_fields);
		print_table_border(sorted_cd_entries, num_fields);

		// Sort records if necessary.
		if (has_order_by_clause) {
			sort_records(record_rows, num_loaded_records, &cd_entries[order_by_column_id], order_by_desc);
		}

		// Print all sorted records.
		for (int i = 0; i < num_loaded_records; i++) {
			print_record_row(sorted_cd_entries, num_fields, &record_rows[i]);
		}
		print_table_border(sorted_cd_entries, num_fields);
	} else {
		// Aggregate result is shown as a 1x1 table.
		print_aggregate_result(aggregate_type, num_fields, aggregate_records_count, aggregate_int_sum, sorted_cd_entries);
	}

	// Clean allocated heap memory.
	free(tab_header);
	for (int i = 0; i < num_loaded_records; i++) {
		for (int j = 0; j < record_rows[i].num_fields; j++) {
			free(record_rows[i].value_ptrs[j]);
		}
	}
	return rc;
}

int sem_delete(token_list *t_list) {
	int rc = 0;
	token_list *cur = t_list;

	// Check table name.
	if (!can_be_identifier(cur)) {
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
		return rc;
	}

	// Check whether the table name exists.
	tpd_entry *tab_entry = get_tpd_from_list(cur->tok_string);
	if (tab_entry == NULL) {
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
		return rc;
	}

	// Get column descriptors.
	cd_entry *cd_entries = NULL;
	get_cd_entries(tab_entry, &cd_entries);

	bool has_where_clause = false;
	record_predicate row_filter;
	memset(&row_filter, '\0', sizeof(row_filter));
	// Parse WHERE clause.
	cur = cur->next;
	if (cur->tok_value == K_WHERE) {
		has_where_clause = true;
		row_filter.type = K_AND;
		row_filter.num_conditions = 1;

		// Parse name of the filtering column.
		cur = cur->next;
		if (can_be_identifier(cur)) {
			int col_index = get_cd_entry_index(cd_entries, tab_entry->num_columns, cur->tok_string);
			if (col_index > -1) {
				row_filter.conditions[0].col_id = col_index;
				row_filter.conditions[0].value_type = ((cd_entries[col_index].col_type == T_INT) ? FIELD_VALUE_TYPE_INT : FIELD_VALUE_TYPE_STRING);
			} else {
				rc = INVALID_COLUMN_NAME;
				cur->tok_value = INVALID;
				return rc;
			}
		} else {
			rc = INVALID_COLUMN_NAME;
			cur->tok_value = INVALID;
			return rc;
		}

		// Parse the operator and operand of the condition.
		cur = cur->next;
		if (cur->tok_value == S_LESS || cur->tok_value == S_GREATER || cur->tok_value == S_EQUAL) {
			row_filter.conditions[0].op_type = cur->tok_value;
			cur = cur->next;
			if (cur->tok_value == INT_LITERAL) {
				if (row_filter.conditions[0].value_type == FIELD_VALUE_TYPE_INT) {
					row_filter.conditions[0].int_data_value = atoi(cur->tok_string);
				} else {
					rc = INVALID_CONDITION_OPERAND;
					cur->tok_value = INVALID;
					return rc;
				}
			} else if (cur->tok_value == STRING_LITERAL) {
				if (row_filter.conditions[0].value_type == FIELD_VALUE_TYPE_STRING) {
					strcpy(row_filter.conditions[0].string_data_value, cur->tok_string);
				} else {
					rc = INVALID_CONDITION_OPERAND;
					cur->tok_value = INVALID;
					return rc;
				}
			} else {
				rc = INVALID_CONDITION;
				cur->tok_value = INVALID;
				return rc;
			}
		} else if (cur->tok_value == K_IS && cur->next->tok_value == K_NULL) { // "IS NULL"
			cur = cur->next;
			row_filter.conditions[0].op_type = K_IS;
		} else if (cur->tok_value == K_IS && cur->next->tok_value == K_NOT && cur->next->next->tok_value == K_NULL) { // "IS NOT NULL"
			cur = cur->next->next;
			row_filter.conditions[0].op_type = K_NOT;
		} else {
			rc = INVALID_CONDITION;
			cur->tok_value = INVALID;
			return rc;
		}
		cur = cur->next;
	}

	if (cur->tok_value != EOC) {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}


	// It is the heap memory owner of the content of the whole table.
	// Do not forget to free it later.
	table_file_header *tab_header = NULL;
	if ((rc = load_table_records(tab_entry, &tab_header)) != 0) {
		return rc;
	}

	// Load record rows.
	char *record_in_table = NULL;
	get_table_records(tab_header, &record_in_table);
	record_row *p_first_row = NULL;
	record_row *p_current_row = NULL;
	record_row *p_previous_row = NULL;
	int num_loaded_records = 0;
	int num_affected_records = 0;
	for (int i = 0; i < tab_header->num_records; i++) {
		// Fill all field values (not only displayed columns) from current record.
		if (p_previous_row == NULL) { // The first record will be loaded.
			p_current_row = (record_row *) malloc(sizeof(record_row));
			p_first_row = p_current_row;
		} else {
			p_current_row = (record_row *) malloc(sizeof(record_row));
			p_previous_row->next = p_current_row;
		}
		fill_record_row(cd_entries, tab_entry->num_columns, p_current_row, record_in_table);
		num_loaded_records++;

		// Delete qualified records.
		if ((!has_where_clause) || apply_row_predicate(cd_entries, tab_entry->num_columns, p_current_row, &row_filter)) {
			free_record_row(p_current_row, false);
			p_current_row = NULL;
			if (p_previous_row == NULL) {
				p_first_row = NULL;
			} else {
				p_previous_row->next = NULL;
			}
			num_affected_records++;
		} else {
			p_previous_row = p_current_row;
		}

		// Move forward to next record.
		record_in_table += tab_header->record_size;
	}

	printf("Affected records: %d\n", num_affected_records);
	if (num_affected_records > 0) {
		// Write records back to .tab file.
		rc = save_records_to_file(tab_header, p_first_row);
	} else {
		printf("[warning] No records were deleted.\n");
	}

	free_record_row(p_first_row, true);
	free(tab_header);
	return rc;
}

int sem_update(token_list *t_list) {
	int rc = 0;
	token_list *cur = t_list;
	
	if (!can_be_identifier(cur)) {
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
		return rc;
	}

	// Check whether the table name exists.
	tpd_entry *tab_entry = get_tpd_from_list(cur->tok_string);
	if (tab_entry == NULL) {
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
		return rc;
	}

	// Get column descriptors.
	cd_entry *cd_entries = NULL;
	get_cd_entries(tab_entry, &cd_entries);

	cur = cur->next;
	if (cur->tok_value != K_SET) {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	// Parse field name of the value to be updated.
	cur = cur->next;
	field_value value_to_update;
	bool value_to_update_can_be_null = false;
	memset(&value_to_update, '\0', sizeof(value_to_update));
	if (can_be_identifier(cur)) {
		int col_index = get_cd_entry_index(cd_entries, tab_entry->num_columns, cur->tok_string);
		if (col_index > -1) {
			value_to_update.col_id = col_index;
			value_to_update.linked_token = cur;
			value_to_update.type = ((cd_entries[col_index].col_type == T_INT) ? FIELD_VALUE_TYPE_INT : FIELD_VALUE_TYPE_STRING);
			value_to_update_can_be_null = !cd_entries[col_index].not_null;
		} else {
			rc = INVALID_COLUMN_NAME;
			cur->tok_value = INVALID;
			return rc;
		}
	} else {
		rc = INVALID_COLUMN_NAME;
		cur->tok_value = INVALID;
		return rc;
	}

	cur = cur->next;
	if (cur->tok_value != S_EQUAL) {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	// Parse field value of the value to be updated.
	cur = cur->next;
	if (cur->tok_value == STRING_LITERAL) {
		if (value_to_update.type == FIELD_VALUE_TYPE_STRING) {
			strcpy(value_to_update.string_value, cur->tok_string);
			value_to_update.is_null = false;
		} else {
			rc = DATA_TYPE_MISMATCH;
			cur->tok_value = INVALID;
			return rc;
		}
	} else if (cur->tok_value == INT_LITERAL) {
		if (value_to_update.type == FIELD_VALUE_TYPE_INT) {
			value_to_update.int_value = atoi(cur->tok_string);
			value_to_update.is_null = false;
		} else {
			rc = DATA_TYPE_MISMATCH;
			cur->tok_value = INVALID;
			return rc;
		}
	} else if (cur->tok_value == K_NULL) {
		if (value_to_update_can_be_null) {
			value_to_update.is_null = true;
		} else {
			rc = UNEXPECTED_NULL_VALUE;
			cur->tok_value = INVALID;
			return rc;
		}
	} else {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	bool has_where_clause = false;
	record_predicate row_filter;
	memset(&row_filter, '\0', sizeof(row_filter));
	// Parse WHERE clause.
	cur = cur->next;
	if (cur->tok_value == K_WHERE) {
		has_where_clause = true;
		row_filter.type = K_AND;
		row_filter.num_conditions = 1;

		// Parse name of the filtering column.
		cur = cur->next;
		if (can_be_identifier(cur)) {
			int col_index = get_cd_entry_index(cd_entries, tab_entry->num_columns, cur->tok_string);
			if (col_index > -1) {
				row_filter.conditions[0].col_id = col_index;
				row_filter.conditions[0].value_type = ((cd_entries[col_index].col_type == T_INT) ? FIELD_VALUE_TYPE_INT : FIELD_VALUE_TYPE_STRING);
			} else {
				rc = INVALID_COLUMN_NAME;
				cur->tok_value = INVALID;
				return rc;
			}
		} else {
			rc = INVALID_COLUMN_NAME;
			cur->tok_value = INVALID;
			return rc;
		}

		// Parse the operator and operand of the condition.
		cur = cur->next;
		if (cur->tok_value == S_LESS || cur->tok_value == S_GREATER || cur->tok_value == S_EQUAL) {
			row_filter.conditions[0].op_type = cur->tok_value;
			cur = cur->next;
			if (cur->tok_value == INT_LITERAL) {
				if (row_filter.conditions[0].value_type == FIELD_VALUE_TYPE_INT) {
					row_filter.conditions[0].int_data_value = atoi(cur->tok_string);
				} else {
					rc = INVALID_CONDITION_OPERAND;
					cur->tok_value = INVALID;
					return rc;
				}
			} else if (cur->tok_value == STRING_LITERAL) {
				if (row_filter.conditions[0].value_type == FIELD_VALUE_TYPE_STRING) {
					strcpy(row_filter.conditions[0].string_data_value, cur->tok_string);
				} else {
					rc = INVALID_CONDITION_OPERAND;
					cur->tok_value = INVALID;
					return rc;
				}
			} else {
				rc = INVALID_CONDITION;
				cur->tok_value = INVALID;
				return rc;
			}
		} else if (cur->tok_value == K_IS && cur->next->tok_value == K_NULL) { // "IS NULL"
			cur = cur->next;
			row_filter.conditions[0].op_type = K_IS;
		} else if (cur->tok_value == K_IS && cur->next->tok_value == K_NOT && cur->next->next->tok_value == K_NULL) { // "IS NOT NULL"
			cur = cur->next->next;
			row_filter.conditions[0].op_type = K_NOT;
		} else {
			rc = INVALID_CONDITION;
			cur->tok_value = INVALID;
			return rc;
		}
		cur = cur->next;
	}
	
	if (cur->tok_value != EOC) {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	// It is the heap memory owner of the content of the whole table.
	// Do not forget to free it later.
	table_file_header *tab_header = NULL;
	if ((rc = load_table_records(tab_entry, &tab_header)) != 0) {
		return rc;
	}

	// Load record rows.
	char *record_in_table = NULL;
	get_table_records(tab_header, &record_in_table);
	record_row record_rows[MAX_NUM_ROW];
	memset(record_rows, '\0', sizeof(record_rows));
	record_row *p_current_row = NULL;
	int num_loaded_records = 0;
	int num_affected_records = 0;
	for (int i = 0; i < tab_header->num_records; i++) {
		// Fill all field values (not only displayed columns) from current record.
		p_current_row = &record_rows[num_loaded_records];
		fill_record_row(cd_entries, tab_entry->num_columns, p_current_row, record_in_table);
		num_loaded_records++;

		// Update qualified records.
		if ((!has_where_clause) || apply_row_predicate(cd_entries, tab_entry->num_columns, p_current_row, &row_filter)) {
			// Only update the record if the value is really changed.
			bool value_changed = false;
			if (value_to_update.type == FIELD_VALUE_TYPE_INT) {
				if (value_to_update.is_null) {
					// Update NULL integer value.
					if (!p_current_row->value_ptrs[value_to_update.col_id]->is_null) {
						p_current_row->value_ptrs[value_to_update.col_id]->is_null = true;
						p_current_row->value_ptrs[value_to_update.col_id]->int_value = 0;
						value_changed = true;
					}
				} else {
					// Update real integer value.
					if (p_current_row->value_ptrs[value_to_update.col_id]->is_null || 
						p_current_row->value_ptrs[value_to_update.col_id]->int_value != value_to_update.int_value) {
							p_current_row->value_ptrs[value_to_update.col_id]->is_null = false;
							p_current_row->value_ptrs[value_to_update.col_id]->int_value = value_to_update.int_value;
							value_changed = true;
					}
				}
			} else {
				if (value_to_update.is_null) {
					// Update NULL string value.
					if (!p_current_row->value_ptrs[value_to_update.col_id]->is_null) {
						p_current_row->value_ptrs[value_to_update.col_id]->is_null = true;
						memset(p_current_row->value_ptrs[value_to_update.col_id]->string_value, '\0', MAX_STRING_LEN+1);
						value_changed = true;
					}
				} else {
					// Update real string value.
					if (p_current_row->value_ptrs[value_to_update.col_id]->is_null ||
						strcmp(p_current_row->value_ptrs[value_to_update.col_id]->string_value, value_to_update.string_value) != 0) {
							p_current_row->value_ptrs[value_to_update.col_id]->is_null = false;
							strcpy(p_current_row->value_ptrs[value_to_update.col_id]->string_value, value_to_update.string_value);
							value_changed = true;
					}
				}
			}
			if (value_changed) {
				fill_raw_record_bytes(cd_entries, p_current_row->value_ptrs, tab_entry->num_columns, record_in_table, tab_header->record_size);
				num_affected_records++;
			}
		}

		// Move forward to next record.
		record_in_table += tab_header->record_size;
	}

	printf("Affected records: %d\n", num_affected_records);

	if (num_affected_records > 0) {
		// Write records back to .tab file.
		char table_filename[MAX_IDENT_LEN + 5];
		sprintf(table_filename, "%s.tab", tab_header->tpd_ptr->table_name);
		FILE *fhandle = NULL;
		if ((fhandle = fopen(table_filename, "wbc")) == NULL) {
			rc = FILE_OPEN_ERROR;
		} else {
			tab_header->tpd_ptr = NULL; // Reset tpd pointer.
			fwrite(tab_header, tab_header->file_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);
		}
	} else {
		printf("[warning] No records were updated.\n");
	}

	free(tab_header);
	return rc;
}

int create_tab_file(char* table_name, cd_entry cd_entries[], int num_columns) {
	int rc = 0;
	table_file_header tab_header;

	int record_size = 0;
	for (int i = 0; i < num_columns; i++) {
		record_size += (1 + cd_entries[i].col_len);
	}
	// The total record_size must be rounded to a 4-byte boundary.
	record_size = round_integer(record_size, 4);

	tab_header.file_size = sizeof(table_file_header);
	tab_header.record_size = record_size;
	tab_header.num_records = 0;
	tab_header.record_offset = sizeof(table_file_header);
	tab_header.file_header_flag = 0;
	tab_header.tpd_ptr = NULL; // Reset tpd pointer.
	
	char table_filename[MAX_IDENT_LEN + 5];
	sprintf(table_filename, "%s.tab", table_name);
	FILE *fhandle = NULL;

	if ((fhandle = fopen(table_filename, "wbc")) == NULL) {
		rc = FILE_OPEN_ERROR;
	} else {
		fwrite((void *)&tab_header, tab_header.file_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int check_insert_values(field_value field_values[], int num_values, cd_entry cd_entries[], int num_columns) {
	int rc = 0;
	if (num_values != num_columns) {
		return INVALID_VALUES_COUNT;
	}

	for (int i = 0; i < num_values; i++) {
		if (field_values[i].is_null) {
			// Field value is NULL.
			if (cd_entries[i].col_type == T_INT) {
				field_values[i].type = FIELD_VALUE_TYPE_INT;
			} else {
				field_values[i].type = FIELD_VALUE_TYPE_STRING;
			}
			if (cd_entries[i].not_null) {
				field_values[i].linked_token->tok_value = INVALID;
				rc = UNEXPECTED_NULL_VALUE;
				break;
			}
		} else {
			// Field value is not NULL, so it must be either integer or string.
			if (field_values[i].type == FIELD_VALUE_TYPE_INT) {
				if (cd_entries[i].col_type != T_INT) {
					field_values[i].linked_token->tok_value = INVALID;
					rc = DATA_TYPE_MISMATCH;
					break;
				}
			} else if (field_values[i].type == FIELD_VALUE_TYPE_STRING) {
				if ((cd_entries[i].col_type != T_CHAR) || (cd_entries[i].col_len < (int)strlen(field_values[i].string_value))) {
					field_values[i].linked_token->tok_value = INVALID;
					rc = DATA_TYPE_MISMATCH;
					break;
				}
			}
		}
	}
	return rc;
}

void free_token_list(token_list* const t_list) {
	token_list* tok_ptr = t_list;
	token_list* tmp_tok_ptr = NULL;
	while (tok_ptr != NULL) {
		tmp_tok_ptr = tok_ptr->next;
		free(tok_ptr);
		tok_ptr = tmp_tok_ptr;
	}
}

int load_table_records(tpd_entry *tpd, table_file_header **pp_table_header) {
	int rc = 0;
	char table_filename[MAX_IDENT_LEN + 5];
	sprintf(table_filename, "%s.tab", tpd->table_name);
	FILE *fhandle = NULL;
	if ((fhandle = fopen(table_filename, "rbc")) == NULL) {
		return FILE_OPEN_ERROR;
	}
	int file_size = get_file_size(fhandle);
	table_file_header *tab_header = (table_file_header *) malloc(file_size);
	fread(tab_header, file_size, 1, fhandle);
	fclose(fhandle);
	if (tab_header->file_size != file_size) {
		rc = TABFILE_CORRUPTION;
		free(tab_header);
	}
	tab_header->tpd_ptr = tpd;
	*pp_table_header = tab_header;
	return rc;
}

int get_file_size(FILE *fhandle) {
	if (!fhandle) {
		return -1;
	}
	struct _stat file_stat;
	_fstat(_fileno(fhandle), &file_stat);
	return (int)(file_stat.st_size);
}

int fill_raw_record_bytes(cd_entry cd_entries[], field_value *field_values[], int num_cols, char record_bytes[], int num_record_bytes) {
	memset(record_bytes, '\0', num_record_bytes);
	unsigned char value_length = 0;
	int cur_offset_in_record = 0;
	int int_value = 0;
	char *string_value = NULL;
	for (int i = 0; i < num_cols; i++) {
		if (field_values[i]->type == FIELD_VALUE_TYPE_INT) {
			// Store a integer.
			if (field_values[i]->is_null) {
				// Null value.
				value_length = 0;
				memcpy(record_bytes + cur_offset_in_record, &value_length, 1);
				cur_offset_in_record += (1 + cd_entries[i].col_len);
			} else {
				// Integer value.
				int_value = field_values[i]->int_value;
				value_length = cd_entries[i].col_len;
				memcpy(record_bytes + cur_offset_in_record, &value_length, 1);
				cur_offset_in_record += 1;
				memcpy(record_bytes + cur_offset_in_record, &int_value, value_length);
				cur_offset_in_record += cd_entries[i].col_len;
			}
		} else {
			// Store a string.
			if (field_values[i]->is_null) {
				// Null value.
				value_length = 0;
				memcpy(record_bytes + cur_offset_in_record, &value_length, 1);
				cur_offset_in_record += (1 + cd_entries[i].col_len);
			} else {
				// String value.
				string_value = field_values[i]->string_value;
				value_length = strlen(string_value);
				memcpy(record_bytes + cur_offset_in_record, &value_length, 1);
				cur_offset_in_record += 1;
				memcpy(record_bytes + cur_offset_in_record, string_value, strlen(string_value));
				cur_offset_in_record += cd_entries[i].col_len;
			}
		}
	}
	return cur_offset_in_record;
}

int fill_record_row(cd_entry cd_entries[], int num_cols, record_row *p_row, char record_bytes[]) {
	memset(p_row, '\0', sizeof(record_row));

	int offset_in_record = 0;
	unsigned char value_length = 0;
	field_value *p_field_value = NULL;
	for (int i = 0; i < num_cols; i++) {
		// Get value length.
		memcpy(&value_length, record_bytes + offset_in_record, 1);
		offset_in_record += 1;

		// Get field value.
		p_field_value = (field_value *) malloc(sizeof(field_value));
		p_field_value->col_id = cd_entries[i].col_id;
		p_field_value->is_null = (value_length == 0);
		p_field_value->linked_token = NULL;
		if (cd_entries[i].col_type == T_INT) {
			// Set an integer.
			p_field_value->type = FIELD_VALUE_TYPE_INT;
			if (!p_field_value->is_null) {
				memcpy(&p_field_value->int_value, record_bytes + offset_in_record, value_length);
			}
		} else {
			// Set a string.
			p_field_value->type = FIELD_VALUE_TYPE_STRING;
			if (!p_field_value->is_null) {
				memcpy(p_field_value->string_value, record_bytes + offset_in_record, value_length);
				p_field_value->string_value[value_length] = '\0';
			}
		}
		p_row->value_ptrs[i] = p_field_value;
		offset_in_record += cd_entries[i].col_len;
	}
	p_row->num_fields = num_cols;
	p_row->sorting_col_id = -1;
	p_row->next = NULL;
	return offset_in_record;
}

void print_table_border(cd_entry *sorted_cd_entries[], int num_values) {
	int col_width = 0;
	for (int i = 0; i < num_values; i++) {
		printf("+");
		col_width = column_display_width(sorted_cd_entries[i]);
		repeat_print_char('-', col_width + 2);
	}
	printf("+\n");
}

void print_table_column_names(cd_entry *sorted_cd_entries[], field_name field_names[], int num_values) {
	int col_gap = 0;
	for (int i = 0; i < num_values; i++) {
		printf("%c %s", '|', field_names[i].name);
		col_gap = column_display_width(sorted_cd_entries[i]) - strlen(sorted_cd_entries[i]->col_name) + 1;
		repeat_print_char(' ', col_gap);
	}
	printf("%c\n", '|');
}

int column_display_width(cd_entry *col_entry) {
	int col_name_len = strlen(col_entry->col_name);
	if (col_entry->col_len > col_name_len) {
		return col_entry->col_len;
	} else {
		return col_name_len;
	}
}

void print_record_row(cd_entry *sorted_cd_entries[], int num_cols, record_row *row) {
	int col_gap = 0;
	char display_value[MAX_STRING_LEN + 1];
	int col_index = -1;
	field_value **field_values = row->value_ptrs;
	bool left_align = true;
	for (int i = 0; i < num_cols; i++) {
		col_index = sorted_cd_entries[i]->col_id;
		left_align = true;
		if (!field_values[col_index]->is_null) {
			if (field_values[col_index]->type == FIELD_VALUE_TYPE_INT) {
				left_align = false;
				sprintf(display_value, "%d", field_values[col_index]->int_value);
			} else {
				strcpy(display_value, field_values[col_index]->string_value);
			}
		} else {
			// Display NULL value as a dash.
			strcpy(display_value, "-");
			left_align = (field_values[col_index]->type == FIELD_VALUE_TYPE_STRING);
		}
		col_gap = column_display_width(sorted_cd_entries[i]) - strlen(display_value) + 1;
		if (left_align) {
			printf("| %s", display_value);
			repeat_print_char(' ', col_gap);
		} else {
			printf("|");
			repeat_print_char(' ', col_gap);
			printf("%s ", display_value);
		}
	}
	printf("%c\n", '|');
}

int get_cd_entry_index(cd_entry cd_entries[], int num_cols, char *col_name) {
	for (int i = 0; i < num_cols; i++) {
		// Column names are case sensitive.
		if (strcmp(cd_entries[i].col_name, col_name) == 0) {
			return i;
		}
	}
	return -1;
}

void print_aggregate_result(int aggregate_type, int num_fields, int records_count, int int_sum, cd_entry* sorted_cd_entries[]) {
	char display_value[MAX_STRING_LEN + 1];
	memset(display_value, '\0', sizeof(display_value));
	if (aggregate_type == F_SUM) {
		sprintf(display_value, "%d", int_sum);
	} else if (aggregate_type == F_AVG) {
		if (records_count == 0) {
			// Divided by zero error, show as NaN (i.e. Not-a-number).
			sprintf(display_value, "NaN");
		} else {
			sprintf(display_value, "%d", int_sum / records_count);
		}
	} else if (aggregate_type == F_COUNT) {
		sprintf(display_value, "%d", records_count);
	}

	char display_title[MAX_STRING_LEN + 1];
	memset(display_title, '\0', sizeof(display_title));
	if (num_fields == 1) {
		// Aggregate on one column.
		if (aggregate_type == F_SUM) {
			sprintf(display_title, "SUM(%s)", sorted_cd_entries[0]->col_name);
		} else if (aggregate_type == F_AVG) {
			sprintf(display_title, "AVG(%s)", sorted_cd_entries[0]->col_name);
		} else { // F_COUNT
			sprintf(display_title, "COUNT(%s)", sorted_cd_entries[0]->col_name);
		}
	} else {
		// Aggregate on one column.
		sprintf(display_title, "COUNT(*)");
	}

	int display_width = (strlen(display_value) > strlen(display_title)) ? strlen(display_value) : strlen(display_title);
	printf("+");
	repeat_print_char('-', display_width + 2);
	printf("+\n");

	printf("| %s ", display_title);
	repeat_print_char(' ', display_width - strlen(display_title));
	printf("|\n");

	printf("+");
	repeat_print_char('-', display_width + 2);
	printf("+\n");

	printf("| %s ", display_value);
	repeat_print_char(' ', display_width - strlen(display_value));
	printf("|\n");

	printf("+");
	repeat_print_char('-', display_width + 2);
	printf("+\n");
}

bool apply_row_predicate(cd_entry cd_entries[], int num_cols, record_row *p_row, record_predicate *p_predicate) {
	if ((!p_predicate) || (p_predicate->num_conditions < 1)) {
		return true;
	}

	// Evaluate the first condition.
	field_value *lhs_operand = p_row->value_ptrs[p_predicate->conditions[0].col_id];
	bool result = eval_condition(&p_predicate->conditions[0], lhs_operand);
	if (p_predicate->num_conditions == 1) {
		// Only one condition.
		return result;
	}

	// Evaluate the second condition.
	lhs_operand = p_row->value_ptrs[p_predicate->conditions[1].col_id];
	if (p_predicate->type == K_AND) {
		// AND conditions.
		result = result && eval_condition(&p_predicate->conditions[1], lhs_operand);
	} else {
		// OR conditions.
		result = result || eval_condition(&p_predicate->conditions[1], lhs_operand);
	}

	return result;
}

bool eval_condition(record_condition *p_condition, field_value *p_field_value) {
	bool result = true;
	switch (p_condition->op_type) {
	case S_LESS:
		// Operator "<"
		if (p_condition->value_type == FIELD_VALUE_TYPE_INT) {
			if (p_field_value->is_null) {
				result = false;
			} else {
				result = (p_field_value->int_value < p_condition->int_data_value);
			}
		} else {
			if (p_field_value->is_null) {
				result = false;
			} else {
				result = (strcmp(p_field_value->string_value, p_condition->string_data_value) < 0);
			}
		}
		break;
	case S_EQUAL:
		// Operator "="
		if (p_condition->value_type == FIELD_VALUE_TYPE_INT) {
			if (p_field_value->is_null) {
				result = false;
			} else {
				result = (p_field_value->int_value == p_condition->int_data_value);
			}
		} else {
			if (p_field_value->is_null) {
				result = false;
			} else {
				result = (strcmp(p_field_value->string_value, p_condition->string_data_value) == 0);
			}
		}
		break;
	case S_GREATER:
		// Operator ">"
		if (p_condition->value_type == FIELD_VALUE_TYPE_INT) {
			if (p_field_value->is_null) {
				result = false;
			} else {
				result = (p_field_value->int_value > p_condition->int_data_value);
			}
		} else {
			if (p_field_value->is_null) {
				result = false;
			} else {
				result = (strcmp(p_field_value->string_value, p_condition->string_data_value) > 0);
			}
		}
		break;
	case K_IS:
		// Operator "IS_NULL"
		result = p_field_value->is_null;
		break;
	case K_NOT:
		// Operator "IS_NOT_NULL"
		result = !p_field_value->is_null;
		break;
	default:
		// Return true for unknown relational operators.
		printf("[warning] unknown relational operator: %d\n", p_condition->op_type);
		result = true;
	}
	return result;
}

void sort_records(record_row rows[], int num_records, cd_entry *p_sorting_col, bool is_desc) {
	for (int i = 0; i < num_records; i++) {
		rows[i].sorting_col_id = p_sorting_col->col_id;
	}
	qsort(rows, num_records, sizeof(record_row), records_comparator);
	record_row temp_record;
	if (is_desc) {
		for (int i = 0; i < num_records / 2; i++) {
			// temp_record := rows[i];
			memcpy(&temp_record, &rows[i], sizeof(record_row));
			// rows[i] := rows[num_records - 1 - i];
			memcpy(&rows[i], &rows[num_records - 1 - i], sizeof(record_row));
			// rows[num_records - 1 - i] := temp_record;
			memcpy(&rows[num_records - 1 - i], &temp_record, sizeof(record_row));
		}
	}
}

int records_comparator(const void *arg1, const void *arg2) {
	record_row *p_record1 = (record_row *) arg1;
	record_row *p_record2 = (record_row *) arg2;
	int sorting_col_id = p_record1->sorting_col_id;

	// If result < 0, elem1 less than elem2;
    // If result = 0, elem1 equivalent to elem2;
    // If result > 0, elem1 greater than elem2.
	int result = 0;
	field_value *p_value1 = p_record1->value_ptrs[sorting_col_id];
	field_value *p_value2 = p_record2->value_ptrs[sorting_col_id];
	if (p_value1->type == FIELD_VALUE_TYPE_INT) {
		// Compare 2 integers.
		// NULL is smaller than any other values.
		if (p_value1->is_null) {
			result = (p_value2->is_null ? 0 : -1);
		} else {
			if (p_value2->is_null) {
				result = 1;
			} else if (p_value1->int_value < p_value2->int_value) {
				result = -1;
			} else if (p_value1->int_value > p_value2->int_value) {
				result = 1;
			} else {
				result = 0;
			}
		}
	} else {
		// Compare 2 strings.
		// NULL is smaller than any other values.
		if (p_value1->is_null) {
			result = (p_value2->is_null ? 0 : -1);
		} else {
			if (p_value2->is_null) {
				result = 1;
			} else {
				result = strcmp(p_value1->string_value, p_value2->string_value);
			}
		}
	}
	return result;
}

void free_record_row(record_row *row, bool to_last) {
	record_row *current_row = row;
	record_row *temp = NULL;
	while (current_row) {
		for (int i = 0; i < current_row->num_fields; i++) {
			free(current_row->value_ptrs[i]);
		}
		if (!to_last) {
			return;
		}
		temp = current_row;
		current_row = current_row->next;
		free(temp);
	}
}

int save_records_to_file(table_file_header * const tab_header, record_row * const rows_head) {
	int rc = 0;
	tpd_entry * const tab_entry = tab_header->tpd_ptr;

	int num_rows = 0;
	record_row *current_row = rows_head;
	while (current_row) {
		num_rows++;
		current_row = current_row->next;
	}

	tab_header->num_records = num_rows;
	tab_header->file_size = sizeof(table_file_header) + tab_header->record_size * num_rows;
	tab_header->tpd_ptr = NULL; // Reset tpd pointer.

	char table_filename[MAX_IDENT_LEN + 5];
	sprintf(table_filename, "%s.tab", tab_entry->table_name);
	FILE *fhandle = NULL;
	if ((fhandle = fopen(table_filename, "wbc")) == NULL) {
		rc = FILE_OPEN_ERROR;
	} else {
		fwrite(tab_header, tab_header->record_offset, 1, fhandle);

		char *record_raw_bytes = (char *) malloc(tab_header->record_size);
		cd_entry *cd_entries = NULL;
		get_cd_entries(tab_entry, &cd_entries);
		current_row = rows_head;
		while (current_row) {
			memset(record_raw_bytes, '\0', tab_header->record_size);
			fill_raw_record_bytes(cd_entries, current_row->value_ptrs, tab_entry->num_columns, record_raw_bytes, tab_header->record_size);
			fwrite(record_raw_bytes, tab_header->record_size, 1, fhandle);
			current_row = current_row->next;
		}
		free(record_raw_bytes);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int write_log_with_timestamp(const char *msg, time_t timestamp) {
	FILE *fhandle = fopen("db.log", "a");
	if (!fhandle) {
		return FILE_OPEN_ERROR;
	}

	char timestamp_text[MAX_STRING_LEN];
	// Convert time to struct tm form in GMT.
	struct tm *t = localtime(&timestamp);
	// Format is "yyyymmddhhmmss".
	sprintf(timestamp_text, "%d%02d%02d%02d%02d%02d", 1900 + t->tm_year, 1 + t->tm_mon, t->tm_mday, t->tm_hour, t->tm_min, t->tm_sec);
	fprintf(fhandle, "%s \"%s\"\n", timestamp_text, msg);
	fclose(fhandle);
	return 0;
}
