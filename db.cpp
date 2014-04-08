/************************************************************
Project#1:	CLP & DDL
************************************************************/

#include "db.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>

int main(int argc, char** argv) {
	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL;

	if ((argc != 2) || (strlen(argv[1]) == 0)) {
		printf("Usage: db \"command statement\"");
		return 1;
	}

	rc = initialize_tpd_list();

	if (rc) {
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
	} else {
		rc = get_token(argv[1], &tok_list);

		/* Test code */
		tok_ptr = tok_list;
		while (tok_ptr != NULL) {
			printf("%16s \t%d \t %d\n", tok_ptr->tok_string, tok_ptr->tok_class, tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}

		if (!rc) {
			rc = do_semantic(tok_list);
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

		/* Whether the token list is valid or not, we need to free the memory */
		free_token_list(tok_list);
	}

	// Free g_tpd_list since all changes have been stored in files.
	if (g_tpd_list) {
		free(g_tpd_list);
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
			int t_class;
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

int do_semantic(token_list *tok_list)
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
	} else {
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd != INVALID_STATEMENT) {
		switch(cur_cmd) {
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
	if ((cur->tok_class != TOKEN_CLASS_KEYWORD) &&
		(cur->tok_class != TOKEN_CLASS_IDENTIFIER) &&
		(cur->tok_class != TOKEN_CLASS_TYPE_NAME)) {
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
					if ((cur->tok_class != TOKEN_CLASS_KEYWORD) &&
						(cur->tok_class != TOKEN_CLASS_IDENTIFIER) &&
						(cur->tok_class != TOKEN_CLASS_TYPE_NAME)) {
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					} else {
						int i;
						for(i = 0; i < cur_id; i++) {
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
						if(!rc){
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
	if ((cur->tok_class != TOKEN_CLASS_KEYWORD) &&
		(cur->tok_class != TOKEN_CLASS_IDENTIFIER) &&
		(cur->tok_class != TOKEN_CLASS_TYPE_NAME)) {
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
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
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

		if ((cur->tok_class != TOKEN_CLASS_KEYWORD) &&
			(cur->tok_class != TOKEN_CLASS_IDENTIFIER) &&
			(cur->tok_class != TOKEN_CLASS_TYPE_NAME))
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

					if ((cur->tok_class != TOKEN_CLASS_KEYWORD) &&
						(cur->tok_class != TOKEN_CLASS_IDENTIFIER) &&
						(cur->tok_class != TOKEN_CLASS_TYPE_NAME))
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
						if((fhandle = fopen(filename, "a+tc")) == NULL)
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
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
							i < tab_entry->num_columns; i++, col_entry++)
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
	if((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		// DB file "dbfile.bin" doesn't exist, so create it.
		if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
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

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
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

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
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
	token_list *cur;
	tpd_entry *tab_entry;

	cur = t_list;
	if ((cur->tok_class != TOKEN_CLASS_KEYWORD) &&
		(cur->tok_class != TOKEN_CLASS_IDENTIFIER) &&
		(cur->tok_class != TOKEN_CLASS_TYPE_NAME)) {
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
			return rc;
	}

	// Check whether the table name exists.
	tab_entry = get_tpd_from_list(cur->tok_string);

	if (tab_entry == NULL) {
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
		return rc;
	}

	cur = cur->next;
	if ((cur->tok_value != K_VALUES) && (cur->next->tok_value != S_LEFT_PAREN))	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}

	cur = cur->next->next;
	// Read all the value tokens and store them in a token list.

	bool values_done = false;
	token_list *value_tokens = NULL;
	token_list *current_value_token = NULL;
	while (!values_done) {
		if ((cur->tok_value == STRING_LITERAL) || (cur->tok_value == INT_LITERAL) || (cur->tok_value == K_NULL)) {
			if (value_tokens == NULL) {
				// The pointer current_value_token points to the new created value_token.
				current_value_token = (token_list *) malloc(sizeof(token_list));
				value_tokens = current_value_token;
				current_value_token->tok_class = cur->tok_class;
				current_value_token->tok_value = cur->tok_value;
				strcpy(current_value_token->tok_string, cur->tok_string);
				current_value_token->next = NULL;
			} else {
				// The pointer current_value_token points to the previous value_token.
				current_value_token->next = (token_list *) malloc(sizeof(token_list));
				current_value_token->next->tok_class = cur->tok_class;
				current_value_token->next->tok_value = cur->tok_value;
				strcpy(current_value_token->next->tok_string, cur->tok_string);
				current_value_token->next->next = NULL;
				// Move current_value_token forward.
				current_value_token = current_value_token->next;
			}
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
	}

	if (rc) {
		free_token_list(value_tokens);
		cur->tok_value = INVALID;
		return rc;
	}

	cd_entry * const col_desc_entries = (cd_entry *) (((char *) tab_entry) + tab_entry->cd_offset);
	rc = check_insert_values(value_tokens, col_desc_entries, tab_entry->num_columns);

	if (rc) {
		free_token_list(value_tokens);
		cur->tok_value = INVALID;
		return rc;
	}

	// Now we can append the new record and write the .tab file back.
	table_file_header *table_header;
	rc = load_table_records(tab_entry, &table_header);

	if (rc) {
		free_token_list(value_tokens);
		cur->tok_value = INVALID;
		return rc;
	}

	// Compose the new record.
	// The maximum possible length of each field is 1 (for the data length) + 255 (a string of 255 characters) = 256.
	// For x86 and x64 machines, the default stack size is 1 MB, so it is safe to have 16 * 256 = 4K memory in stack for the new record.
	char record_bytes[MAX_NUM_COL * 256];
	memset(record_bytes, '\0', sizeof(record_bytes));
	unsigned char value_length = 0;
	int cur_offset_in_record = 0;
	current_value_token = value_tokens;
	int int_value = 0;
	char *string_value = NULL;
	for(int i = 0; i < tab_entry->num_columns; i++) {
		if (col_desc_entries[i].col_type == T_INT) {
			// Store a integer.
			if (current_value_token->tok_value == K_NULL) {
				// Null value.
				value_length = 0;
				memcpy(record_bytes + cur_offset_in_record, &value_length, 1);
				cur_offset_in_record += (1 + col_desc_entries[i].col_len);
			} else {
				// Int value.
				int_value = atoi(current_value_token->tok_string);
				value_length = col_desc_entries[i].col_len;
				memcpy(record_bytes + cur_offset_in_record, &value_length, 1);
				cur_offset_in_record += 1;
				memcpy(record_bytes + cur_offset_in_record, &int_value, value_length);
				cur_offset_in_record += col_desc_entries[i].col_len;
			}
		} else {
			// Store a string.
			if (current_value_token->tok_value == K_NULL) {
				// Null value.
				value_length = 0;
				memcpy(record_bytes + cur_offset_in_record, &value_length, 1);
				cur_offset_in_record += (1 + col_desc_entries[i].col_len);
			} else {
				// String value.
				string_value = current_value_token->tok_string;
				value_length = strlen(string_value);
				memcpy(record_bytes + cur_offset_in_record, &value_length, 1);
				cur_offset_in_record += 1;
				memcpy(record_bytes + cur_offset_in_record, string_value, strlen(string_value));
				cur_offset_in_record += col_desc_entries[i].col_len;
			}
		}
		current_value_token = current_value_token->next;
	}

	char table_filename[MAX_IDENT_LEN + 5];
	sprintf(table_filename, "%s.tab", table_header->tpd_ptr->table_name);
	FILE *fhandle = NULL;
	if((fhandle = fopen(table_filename, "wbc")) == NULL) {
		rc = FILE_OPEN_ERROR;
		free_token_list(value_tokens);
		free(table_header);
		cur->tok_value = INVALID;
		return rc;
	}

	// Add one more record in table header.
	int old_table_file_size = table_header->file_size;
	table_header->num_records++;
	table_header->file_size += table_header->record_size;
	table_header->tpd_ptr = NULL;

	fwrite(table_header, old_table_file_size, 1, fhandle);
	fwrite(record_bytes, table_header->record_size, 1, fhandle);
	fflush(fhandle);
	fclose(fhandle);
	free(table_header);
	return rc;
}

int sem_select(token_list *t_list) {
	int rc = 0;
	token_list *cur;
	printf("unimplemented sem_select\n");
	return rc;
}

int sem_delete(token_list *t_list) {
	int rc = 0;
	token_list *cur;
	printf("unimplemented sem_delete\n");
	return rc;
}

int sem_update(token_list *t_list) {
	int rc = 0;
	token_list *cur;
	printf("unimplemented sem_update\n");
	return rc;
}

int create_tab_file(char* table_name, cd_entry* col_entry, int num_columns) {
	int rc = 0;
	table_file_header tab_header;

	int record_size = 0;
	for (int i = 0; i < num_columns; i++) {
		record_size += (1 + col_entry[i].col_len);
	}
	// The total record_size must be rounded to a 4-byte boundary.
	if (record_size % 4 != 0) {
		record_size += (4 - record_size % 4);
	}

	tab_header.file_size = sizeof(table_file_header);
	tab_header.record_size = record_size;
	tab_header.num_records = 0;
	tab_header.record_offset = sizeof(table_file_header);
	tab_header.file_header_flag = 0;
	tab_header.tpd_ptr = NULL;
	
	char table_filename[MAX_IDENT_LEN + 5];
	sprintf(table_filename, "%s.tab", table_name);
	FILE *fhandle = NULL;

	if((fhandle = fopen(table_filename, "wbc")) == NULL) {
		rc = FILE_OPEN_ERROR;
	} else {
		fwrite((void *)&tab_header, tab_header.file_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int check_insert_values(token_list * const value_tokens, cd_entry * const col_entry, int num_columns) {
	int rc = 0;
	int token_count = 0;
	token_list *cur_token = value_tokens;
	while(cur_token) {
		token_count++;
		cur_token = cur_token->next;
	}
	if (token_count != num_columns) {
		return INVALID_VALUES_COUNT;
	}

	cur_token = value_tokens;
	int token_index = 0;
	while(cur_token) {
		if (cur_token->tok_value == INT_LITERAL) {
			if (col_entry[token_index].col_type != T_INT) {
				rc = INVALID_VALUE;
				break;
			}
		} else if (cur_token->tok_value == STRING_LITERAL) {
			if ((col_entry[token_index].col_type != T_CHAR) || (col_entry[token_index].col_len < strlen(cur_token->tok_string))) {
				rc = INVALID_VALUE;
				break;
			}
		} else if (cur_token->tok_value == K_NULL) {
			if (col_entry[token_index].not_null) {
				rc = INVALID_VALUE;
				break;
			}
		} else {
			rc = INVALID_VALUE;
			break;
		}

		cur_token = cur_token->next;
		token_index++;
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
	if((fhandle = fopen(table_filename, "rbc")) == NULL) {
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
