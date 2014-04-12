#include "stdafx.h"
#include "CppUnitTest.h"
#include "../sjsu_cs257/db.h"

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
		remove("dbfile.bin");
		remove("BOOK.tab");
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
			remove("dbfile.bin");
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
			remove("dbfile.bin");
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

		TEST_METHOD(InsertDataTypeMismatch)
		{
			Assert::AreEqual(static_cast<int>(DATA_TYPE_MISMATCH), execute_statement("INSERT INTO BOOK VALUES(1234, 'Peter Harrington', 1337)"), L"Return code");
		}

		TEST_METHOD(InsertNotNullViolation)
		{
			Assert::AreEqual(static_cast<int>(UNEXPECTED_NULL_VALUE), execute_statement("INSERT INTO BOOK VALUES(NULL, 'Peter Harrington', 1337)"), L"Return code");
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
			remove("dbfile.bin");
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

}
