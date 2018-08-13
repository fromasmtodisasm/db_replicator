#include "db_replicator.h"
#include <iostream>
#include <list>
#include "Config.h"
using namespace std;

namespace replication {


	db_replicator::db_replicator(Params &conf)
	{
		Session session(
			SessionOption::USER, conf["db_from"]["user"],
			SessionOption::PWD, conf["db_from"]["pwd"],
			SessionOption::HOST, conf["db_from"]["host"],
			SessionOption::PORT, std::stol(conf["db_from"]["port"].c_str()),
			SessionOption::DB, conf["db_from"]["database"]
		);
		Schema schema(session, (conf["db_from"]["database"]));
		auto tabList = schema.getTables();
		for (const auto &tab : tabList)
		{
			cout << tab.getName() << ">" << endl;
		}

	}


	db_replicator::~db_replicator()
	{
	}

	std::string db_replicator::replicate()
	{
		return std::string();
	}
	std::list<Table> db_replicator::get_table_diff()
	{
		return std::list<Table>();
	}
	std::list<Column> db_replicator::get_column_diff()
	{
		return std::list<Column>();
	}
}