#include "CppUnitTest.h"
#include "../sjsu_cs257/db.h"
#include <string>
#include <fstream>
#include <vector>

using namespace Microsoft::VisualStudio::CppUnitTestFramework;

// Useful link: http://www.codeproject.com/Articles/672322/Native-Unit-Tests-using-Visual-Studio
namespace sjsu_cs257_test
{		
	BEGIN_TEST_MODULE_ATTRIBUTE()
		TEST_MODULE_ATTRIBUTE(L"Project", L"SJSU CS257 DB")
		TEST_MODULE_ATTRIBUTE(L"Owner", L"Shuang Wu")
	END_TEST_MODULE_ATTRIBUTE()

	TEST_MODULE_INITIALIZE(ModuleStartup) {}

	TEST_MODULE_CLEANUP(ModuleFinalize)
	{
		remove(kDbFile);
		remove("BOOK.tab");
		remove("BOOK2.tab");
		remove(kDbLogFile);
		remove("backup_img");
	}

	TEST_CLASS(CreateTest)
	{
	public:

		BEGIN_TEST_CLASS_ATTRIBUTE()
			TEST_CLASS_ATTRIBUTE(L"Descrioption", L"Tests to create table.")
		END_TEST_CLASS_ATTRIBUTE()

		TEST_CLASS_INITIALIZE(ClassInitialize) {}

		TEST_CLASS_CLEANUP(ClassFinalize) {}

		TEST_METHOD_INITIALIZE(MethodInitialize)
		{
			remove(kDbFile);
			remove("BOOK.tab");
		}

		TEST_METHOD_CLEANUP(MethodFinalize)
		{
			free(g_tpd_list);
		}

		TEST_METHOD(CreateSuccessfully)
		{
			Assert::AreEqual(0, execute_statement("CREATE TABLE BOOK(title char(50) NOT NULL, author char(30), copies int)"), L"Return code.");
			reload_global_tpd_list();
			Assert::AreEqual(1, g_tpd_list->num_tables, L"Number of tables.");
			tpd_entry* tab_entry = get_tpd_from_list("BOOK");
			Assert::AreEqual("BOOK", tab_entry->table_name, L"Table name.");
			Assert::AreEqual(3, tab_entry->num_columns, L"Number of columns.");
		}

		TEST_METHOD(ExsitingTable)
		{
			Assert::AreEqual(0, execute_statement("CREATE TABLE BOOK(title char(50) NOT NULL, author char(30), copies int)"), L"Return code.");
			reload_global_tpd_list();
			Assert::AreEqual(static_cast<int>(DUPLICATE_TABLE_NAME), execute_statement("CREATE TABLE BOOK(title char(50), author char(30), copies int)"), L"Return code of deplicated table.");
			reload_global_tpd_list();
			Assert::AreEqual(1, g_tpd_list->num_tables, L"Numbers of table.");
		}
	};

	TEST_CLASS(InsertTest)
	{
	public:

		BEGIN_TEST_CLASS_ATTRIBUTE()
			TEST_CLASS_ATTRIBUTE(L"Descrioption", L"Tests to insert records.")
		END_TEST_CLASS_ATTRIBUTE()

		TEST_METHOD_INITIALIZE(MethodInitialize)
		{
			remove(kDbFile);
			remove("BOOK.tab");
			Assert::AreEqual(0, execute_statement("CREATE TABLE BOOK(title char(50) NOT NULL, author char(30), copies int)"), L"Return code.");
			reload_global_tpd_list();
		}

		TEST_METHOD_CLEANUP(MethodFinalize)
		{
			free(g_tpd_list);
		}

		TEST_METHOD(InsertSuccessfully)
		{
			Assert::AreEqual(0, execute_statement("INSERT INTO BOOK VALUES('Machine Learning in Action', 'Peter Harrington', 1337)"), L"Return code");
			Assert::AreEqual(0, execute_statement("INSERT INTO BOOK VALUES('Master Thesis: Machine Learning', 'unknown', NULL)"), L"Return code");
		}

		TEST_METHOD(Insert_ZeroedBytesBetweenStoredData)
		{
			Assert::AreEqual(0, execute_statement("INSERT INTO BOOK VALUES('A', 'B', NULL)"), L"Return code");
			FILE *f_table = fopen("BOOK.tab", "rb");
			Assert::IsNotNull(f_table);
			fseek(f_table, sizeof(table_file_header), SEEK_SET);
			char byte;
			// "title" field
			fread(&byte, sizeof(char), 1, f_table); // skip 8-bit length
			for (int i = 0; i < 50; i++) {
				fread(&byte, sizeof(char), 1, f_table);
				if (i == 0) {
					Assert::AreEqual('A', byte);
				} else {
					Assert::AreEqual('\0', byte);
				}
			}
			// "author" field
			fread(&byte, sizeof(char), 1, f_table); // skip 8-bit length
			for (int i = 0; i < 50; i++) {
				fread(&byte, sizeof(char), 1, f_table);
				if (i == 0) {
					Assert::AreEqual('B', byte);
				} else {
					Assert::AreEqual('\0', byte);
				}
			}
			// "copies" field
			fread(&byte, sizeof(char), 1, f_table); // skip 8-bit length
			for (int i = 0; i < 4; i++) {
				fread(&byte, sizeof(char), 1, f_table);
				Assert::AreEqual('\0', byte);
			}
			fclose(f_table);
		}

		TEST_METHOD(InsertDataTypeMismatch)
		{
			Assert::AreEqual(static_cast<int>(DATA_TYPE_MISMATCH), execute_statement("INSERT INTO BOOK VALUES(1234, 'Peter Harrington', 1337)"), L"Return code");
		}

		TEST_METHOD(InsertNotNullViolation)
		{
			Assert::AreEqual(static_cast<int>(UNEXPECTED_NULL_VALUE), execute_statement("INSERT INTO BOOK VALUES(NULL, 'Peter Harrington', 1337)"), L"Return code");
		}

		TEST_METHOD(InvalidStatement)
		{
			Assert::AreEqual(static_cast<int>(INVALID_STATEMENT), execute_statement("INSERT INTO BOOK VALUES)'Machine Learning in Action', 'Peter Harrington', 1337)"), L"Return code");
		}
	};

	TEST_CLASS(UpdateTest)
	{
	public:

		BEGIN_TEST_CLASS_ATTRIBUTE()
			TEST_CLASS_ATTRIBUTE(L"Descrioption", L"Tests to update records.")
		END_TEST_CLASS_ATTRIBUTE()

		TEST_METHOD_INITIALIZE(MethodInitialize)
		{
			remove(kDbFile);
			remove("BOOK.tab");
			Assert::AreEqual(0, execute_statement("CREATE TABLE BOOK(title char(50) NOT NULL, author char(30), copies int)"), L"Return code.");
			Assert::AreEqual(0, execute_statement("INSERT INTO BOOK VALUES('Machine Learning in Action', 'Peter Harrington', 1337)"), L"Return code");
			Assert::AreEqual(0, execute_statement("INSERT INTO BOOK VALUES('Master Thesis: Machine Learning', 'unknown', NULL)"), L"Return code");
			reload_global_tpd_list();
		}

		TEST_METHOD_CLEANUP(MethodFinalize)
		{
			free(g_tpd_list);
		}

		TEST_METHOD(UpdateAllSuccessfully)
		{
			Assert::AreEqual(0, execute_statement("UPDATE BOOK SET author='Shuang Wu'"), L"Return code");
			Assert::AreEqual(0, execute_statement("UPDATE BOOK SET author=NULL"), L"Return code");
		}

		TEST_METHOD(UpdateSuccessfully)
		{
			Assert::AreEqual(0, execute_statement("UPDATE BOOK SET author='Shuang Wu' WHERE copies > 0"), L"Return code");
			Assert::AreEqual(0, execute_statement("UPDATE BOOK SET author=NULL WHERE copies IS NULL"), L"Return code");
		}

		TEST_METHOD(UpdateDataTypeMismatch)
		{
			Assert::AreEqual(static_cast<int>(DATA_TYPE_MISMATCH), execute_statement("UPDATE BOOK SET title=1234 WHERE copies > 0"), L"Return code");
		}

		TEST_METHOD(UpdateNotNullViolation)
		{
			Assert::AreEqual(static_cast<int>(UNEXPECTED_NULL_VALUE), execute_statement("UPDATE BOOK SET title=NULL WHERE copies > 0"), L"Return code");
		}
	};

	TEST_CLASS(LogTest)
	{
	public:

		BEGIN_TEST_CLASS_ATTRIBUTE()
			TEST_CLASS_ATTRIBUTE(L"Descrioption", L"Tests to log original DDL/DML statement within double quotes.")
		END_TEST_CLASS_ATTRIBUTE()

		TEST_METHOD_INITIALIZE(MethodInitialize)
		{
			remove(kDbFile);
			remove("BOOK.tab");
			remove("BOOK2.tab");
			remove(kDbLogFile);
			remove("backup_img");
		}

		TEST_METHOD_CLEANUP(MethodFinalize) {}

		TEST_METHOD(LogWithTimestamp)
		{
			time_t ts = 1399249287L; // Sunday, May 04, 2014 5:21:27 PM GMT-7
			std::string msg("any SQL statement");
			write_log_with_timestamp(msg.c_str(), ts);
			std::ifstream input(kDbLogFile);
			std::string line;
			std::string expected_log_entry("20140504172127");
			expected_log_entry.append(" \"");
			expected_log_entry.append(msg);
			expected_log_entry.append("\"");
			int num_lines = 0;
			while (std::getline(input, line)) {
				Assert::AreEqual(expected_log_entry, line, L"Log line");
				num_lines++;
			}
			Assert::AreEqual(1, num_lines, L"Log lines count");
		}

		TEST_METHOD(LogDdlAndDmlStatements)
		{
			std::vector<std::string> statements;
			statements.push_back("CREATE TABLE BOOK(title char(50) NOT NULL, author char(30), copies int)");
			statements.push_back("INSERT INTO BOOK VALUES('Machine Learning in Action', 'Peter Harrington', 1337)");
			statements.push_back("INSERT INTO BOOK VALUES('Master Thesis: Machine Learning', 'unknown', NULL)");
			statements.push_back("UPDATE BOOK SET author='Shuang Wu'");
			statements.push_back("UPDATE BOOK SET title=NULL"); // will not log because return code is not 0
			statements.push_back("SELECT * FROM BOOK"); // will not log because this is not a DDL or DML statement.
			for (std::vector<std::string>::iterator it = statements.begin(); it != statements.end(); ++it) { 
				execute_statement(const_cast<char *>(it->c_str()));
			}
			std::ifstream input(kDbLogFile);
			std::string line;
			int num_lines = 0;
			while (std::getline(input, line)) {
				Assert::AreEqual(statements[num_lines], line.substr(16, line.size() - 17));
				num_lines++;
			}
			int expected_log_lines = statements.size() - 2;
			Assert::AreEqual(expected_log_lines, num_lines, L"Log lines count");
		}

		TEST_METHOD(InvalidBackupStatements)
		{
			Assert::AreEqual(static_cast<int>(INVALID_STATEMENT), execute_statement("BACKUP"), L"Incomplete statement.");
			Assert::AreEqual(static_cast<int>(INVALID_STATEMENT), execute_statement("BACKUP TO imgfile 123"), L"Too many arguments.");
			Assert::AreEqual(static_cast<int>(INVALID_BACKUP_FILENAME), execute_statement("BACKUP TO 123"), L"Not a valid backup filename.");
			Assert::AreEqual(0, execute_statement("BACKUP TO backup_img"), L"Backup is successful.");
			Assert::AreEqual(static_cast<int>(BACKUP_FILE_EXISTS), execute_statement("BACKUP TO backup_img"), L"Backup file already exists.");
		}

		TEST_METHOD(BackupStatement)
		{
			execute_statement("CREATE TABLE BOOK(title char(50) NOT NULL, author char(30), copies int)");
			execute_statement("INSERT INTO BOOK VALUES('Machine Learning in Action', 'Peter Harrington', 1337)");
			execute_statement("CREATE TABLE BOOK2(title char(150) NOT NULL, author char(80), copies int)");
			execute_statement("INSERT INTO BOOK2 VALUES('Master Thesis: Machine Learning', 'unknown', NULL)");
			Assert::AreEqual(0, execute_statement("BACKUP TO backup_img"), L"Return code");

			// Test file sizes.
			FILE *f_dbfile = fopen(kDbFile, "rb");
			Assert::IsNotNull(f_dbfile);
			FILE *f_table1 = fopen("BOOK.tab", "rb");
			Assert::IsNotNull(f_table1);
			FILE *f_table2 = fopen("BOOK2.tab", "rb");
			Assert::IsNotNull(f_table2);
			FILE *f_backup = fopen("backup_img", "rb");
			Assert::IsNotNull(f_backup);
			int dbfile_size = get_file_size(f_dbfile);
			int table1_size = get_file_size(f_table1);
			int table2_size = get_file_size(f_table2);
			int backup_file_size = get_file_size(f_backup);
			char sizes_msg[255];
			sprintf(sizes_msg, "%d, %d, %d, %d", dbfile_size, table1_size, table2_size, backup_file_size);
			Logger::WriteMessage(sizes_msg);
			Assert::AreEqual(dbfile_size + 4 + table1_size + 4 + table2_size, backup_file_size, L"backup file size");

			// Test file content.
			unsigned char byte_in_backup_file, byte_in_source_file;
			int32_t file_size;
			int num_bytes = 0;
			wchar_t msg[255];
			while (fread(&byte_in_source_file, sizeof(byte_in_source_file), 1, f_dbfile) > 0) {
				fread(&byte_in_backup_file, sizeof(byte_in_backup_file), 1, f_backup);
				_itow(num_bytes, msg, 10);
				Assert::AreEqual(byte_in_source_file, byte_in_backup_file, msg);
				num_bytes++;
			}

			Assert::AreEqual(1, static_cast<int>(fread(&file_size, sizeof(file_size), 1, f_backup)), L"read byte count of table1_file_size");
			Assert::AreEqual(table1_size, file_size, L"file size of BOOK.tab");
			num_bytes += sizeof(file_size);

			while (fread(&byte_in_source_file, sizeof(byte_in_source_file), 1, f_table1) > 0) {
				fread(&byte_in_backup_file, sizeof(byte_in_backup_file), 1, f_backup);
				_itow(num_bytes, msg, 10);
				Assert::AreEqual(byte_in_source_file, byte_in_backup_file, msg);
				num_bytes++;
			}

			Assert::AreEqual(1, static_cast<int>(fread(&file_size, sizeof(file_size), 1, f_backup)), L"read byte count of table2_file_size");
			Assert::AreEqual(table2_size, file_size, L"file size of BOOK2.tab");
			num_bytes += sizeof(file_size);

			while(fread(&byte_in_source_file, sizeof(byte_in_source_file), 1, f_table2) > 0) {
				fread(&byte_in_backup_file, sizeof(byte_in_backup_file), 1, f_backup);
				_itow(num_bytes, msg, 10);
				Assert::AreEqual(byte_in_source_file, byte_in_backup_file, msg);
				num_bytes++;
			}
			

			fclose(f_dbfile);
			fclose(f_table1);
			fclose(f_table2);
			fclose(f_backup);
		}
	};

}
