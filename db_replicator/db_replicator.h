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
	using mysqlx::Table;
	using mysqlx::Column;

	class db_replicator
	{
	public:
		db_replicator(Params &conf);
		~db_replicator();
		string replicate();
	private:
		std::list<Table> get_table_diff();
		std::list<Column> get_column_diff();
	private:
		Session *master_session, *slave_session;
		Schema *master_db, *slave_db;
	};
}
