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
	bool comp_columns(const mysqlx::Column &column1, const mysqlx::Column &column2);

	class dbReplicator
	{
	public:
		dbReplicator(Params params);
		~dbReplicator();
		string replicate();
		std::map<string, string> getTableMetaData(mysqlx::Table table);
	private:
		std::string createTable(mysqlx::Table table);
		void processTables(mysqlx::Table table1, mysqlx::Table table2);
		std::string insertRows(mysqlx::Table table, bool is_all, int id);
		std::vector< std::string> getColumnNames(mysqlx::Table table, bool is_conv);
		
		void processRows(mysqlx::RowResult &masterRes, mysqlx::Table &masterTable, mysqlx::RowResult &slaveRes, mysqlx::Table &slaveTable);
		friend bool comp_tables(mysqlx::Table table1, mysqlx::Table table2);

		bool comp_rows( mysqlx::Row &master, mysqlx::Row &slave)
		{
			return ((int)master.get(0) < (int)slave.get(0));
		}
	private:
		Session *master_session, *slave_session;
		Schema *master_db, *slave_db;
		TableList master_table, slave_table;
		Params conf;
	};

}
