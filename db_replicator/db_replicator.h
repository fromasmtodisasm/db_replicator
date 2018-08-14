#pragma once
#include <string>
#include <vector>
#include <map>
#include <list>
#include <xdevapi.h>

#include "Config.h"

namespace replication {

	using std::string;
	using mysqlx::SessionOption;
	using mysqlx::Session;
	using mysqlx::Schema;
	using TableList=std::vector<mysqlx::Table>;
	using mysqlx::Column;

	bool comp_tables(mysqlx::Table table1, mysqlx::Table table2);

	class db_replicator
	{
	public:
		db_replicator(Params params);
		~db_replicator();
		string replicate();
		std::map<string, string> get_table_metadata(mysqlx::Table table);
	private:
		std::string alter_table(std::string table_name, std::string how, std::string what);
		
		std::string create_table(mysqlx::Table table);
		void process_table(mysqlx::Table table);
		std::string insert_table(mysqlx::Table table);
		std::string get_columns_name(mysqlx::Table table, bool is_conv);
		friend bool comp_tables(mysqlx::Table table1, mysqlx::Table table2);
	private:
		Session *master_session, *slave_session;
		Schema *master_db, *slave_db;
		TableList master_table, slave_table;
		Params conf;
	};

}
