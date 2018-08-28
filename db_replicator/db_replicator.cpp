#include "db_replicator.h"
#include <iostream>
#include <algorithm>
#include <iterator>
#include <list>
#include <map>
#include <numeric>
#include "Config.h"
using namespace std;

namespace replication {
	dbReplicator::dbReplicator(Params params) : conf(params)
	{

		master_session = new Session(
			SessionOption::USER, conf["db_from"]["user"],
			SessionOption::PWD, conf["db_from"]["pwd"],
			SessionOption::HOST, conf["db_from"]["host"],
			SessionOption::PORT, std::stol(conf["db_from"]["port"].c_str()),
			SessionOption::DB, conf["db_from"]["database"]
		);
		slave_session = new Session(
			SessionOption::USER, conf["db_from"]["user"],
			SessionOption::PWD, conf["db_from"]["pwd"],
			SessionOption::HOST, conf["db_from"]["host"],
			SessionOption::PORT, std::stol(conf["db_from"]["port"].c_str()),
			SessionOption::DB, conf["db_from"]["database"]
		);
		master_db = new Schema(*master_session, (conf["db_from"]["database"]));
		slave_db = new Schema(*slave_session, (conf["db_to"]["database"]));


	}

	dbReplicator::~dbReplicator()
	{
		delete master_session;
		delete slave_session;
		delete master_db;
		delete slave_db;
	}

	std::map<string, string> dbReplicator::getTableMetaData(mysqlx::Table table)
	{
		std::map<string, string> result;
		mysqlx::RowResult rowResult = master_session->sql(
			"SELECT DATA_TYPE, IFNULL(CHARACTER_MAXIMUM_LENGTH,0) + IFNULL(NUMERIC_PRECISION,0), COLUMN_NAME "
			"FROM INFORMATION_SCHEMA.COLUMNS "
			"WHERE "
			"TABLE_SCHEMA = '" + (string)table.getSchema().getName() + "' and TABLE_NAME = \"" + (string)table.getName() + "\""
		).execute();

		while (const auto column = rowResult.fetchOne())
		{
			stringstream ss;
			ss << column[0];
			ss << "(";
			ss << std::to_string((int)column[1]);
			ss << ")";
			result[column[2]] = ss.str();
		}

		return result;
	}

	std::string dbReplicator::createTable(mysqlx::Table table)
	{
		auto columns = getTableMetaData(table);
		stringstream sqlQuery;

		string tmp;
		if ((tmp = table.getName()) == (std::string)"addres" || (tmp = table.getName()) == (string)"stuff")
		{
			return string();
		}

		sqlQuery << "create table " << table.getName() << "(";
		int column_cnt = 0;
		for (const auto &column : columns)
		{
			if (column_cnt > 0)
			{
				sqlQuery << ",";
			}
			sqlQuery << column.first << " " << column.second;
			column_cnt++;
		}
		sqlQuery << ");" << endl;
		return sqlQuery.str();

	}
	void dbReplicator::processTables(mysqlx::Table table1, mysqlx::Table table2)
	{


		auto masterRes = table1.select("*").orderBy("id").execute();
		auto slaveRes = table2.select("*").orderBy("id").execute();
		mysqlx::col_count_t masterColumn = 0, slaveColumn = 0;
		{
			auto master = getTableMetaData(table1);
			auto slave = getTableMetaData(table2);
			auto masterFirst = master.begin();
			auto slaveFirst = slave.begin();
			auto masterLast = master.end();
			auto slaveLast = slave.end();
			while (masterFirst != masterLast) {

				if (slaveFirst == slaveLast) break;
				if (masterFirst->first < slaveFirst->first) {
					cout << "alter table " << table2.getName() << " add column " << masterFirst->first << " " << masterFirst->second << ";\n";
					masterFirst++;
				}
				else {
					if (!(masterFirst->first < slaveFirst->first)) {

						if (masterFirst->second != slaveFirst->second)
						{
							cout << "--Different types or length" << endl;
							cout << "alter table " << table2.getName() << " MODIFY " << masterFirst->first << " " << masterFirst->second << ";\n";
						}

						++masterFirst;
					}
					else {
						cout << "alter table " << table2.getName() << " add column " << masterFirst->first << " " << masterFirst->second << ";\n";
					}
					// Следующая slave таблица
					++slaveFirst;
				}
			}

			while (masterFirst != masterLast) {
				cout << "alter table " << table2.getName() << " add column " << masterFirst->first << " " << masterFirst->second << ";\n";
				masterFirst++;
			}
		}

		// Сравним стороки
		processRows(masterRes, table1, slaveRes, table2);
	}
	void dbReplicator::processRows(mysqlx::RowResult &masterRes, mysqlx::Table &masterTable, mysqlx::RowResult &slaveRes, mysqlx::Table &slaveTable)
	{
		{
			std::vector<mysqlx::Row> masterRows = masterRes.fetchAll();
			std::vector<mysqlx::Row> slaveRows = slaveRes.fetchAll();

			auto masterRowsFirst = masterRows.begin();
			auto slaveRowsFirst = slaveRows.begin();
			auto masterRowsLast = masterRows.end();
			auto slaveRowsLast = slaveRows.end();

			
			auto masterClmnNames = getColumnNames(masterTable, false);
			auto slaveClmnNames = getColumnNames(slaveTable, false);

			std::map<std::string, std::pair<std::string, std::string>> test;

			while (masterRowsFirst != masterRowsLast && slaveRowsFirst != slaveRowsLast)
			{

				auto masterCnt = masterRes.getColumnCount();
				auto slaveCnt = slaveRes.getColumnCount();

				for (int i = 1; i < masterCnt; i++)
				{
					pair<string, string> tmp((string)masterRowsFirst->get(i), (string)"");
					test[masterClmnNames[i]] = tmp;

				}
				for (int i = 1; i < slaveCnt; i++)
				{

					pair<string, string> tmp = test[slaveClmnNames[i]];
					tmp.second = slaveRowsFirst->get(i);
					test[slaveClmnNames[i]] = tmp;

				}

				if (comp_rows(*masterRowsFirst, *slaveRowsFirst))
				{
					cout << insertRows(masterTable, false, masterRowsFirst->get(0).get<int>());
					masterRowsFirst++;
				}
				else
				{
					if (!comp_rows(*slaveRowsFirst, *masterRowsFirst))
					{
						stringstream update_stmnt;
						stringstream values;
						update_stmnt << "update " << slaveTable.getName() << " set ";
						int update_cnt = 0;
						for (const auto &cur_clmn : slaveClmnNames)
						{

							std::vector<int> indx;
							if (test[cur_clmn].first != test[cur_clmn].second)
							{
								if (update_cnt > 0)
								{
									update_stmnt << ", ";
									values << ", ";
								}
								update_stmnt << cur_clmn << " = " << test[cur_clmn].first;
								update_cnt++;
							}
						}
						if (update_cnt > 0)
						{
							update_stmnt << " where id = " << std::to_string(slaveRowsFirst->get(0).get<int>()) << ";" << endl;
							cout << update_stmnt.str();
						}
						update_stmnt.str("");

						masterRowsFirst++;
					}
					// Игнорируем отсутствующие в мастере строки
					slaveRowsFirst++;
				}
			}
			while (masterRowsFirst != masterRowsLast) {
				cout << insertRows(masterTable, false, masterRowsFirst->get(0).get<int>());
				masterRowsFirst++;
			}
		}
	}

	std::string dbReplicator::insertRows(mysqlx::Table table, bool is_all, int id = 0)
	{
		stringstream sqlQuery;
		stringstream selectQuery;
		stringstream insertColumns;
		string columns;
		std::vector<std::string> vec_columns = getColumnNames(table, true);
		columns = std::accumulate(vec_columns.begin(), vec_columns.end(), columns,
			[](std::string a, std::string b) {
				return a + ',' + b;

		});
			
		columns.erase(0, 1);
		selectQuery << "select " << columns << " from " << master_session->getDefaultSchemaName()
			<< "." << table.getName();
		if (is_all)
		{
			selectQuery << ";";
		}
		else
		{
			selectQuery << " where id = " << id << ";";
		}
		//cout << selectQuery.str() << endl;
		auto selectResult = master_session->sql(
			selectQuery.str()
		).execute();
	
		auto insertList = selectResult.fetchAll();
		for (const auto &insertColumn : insertList)
		{
			for (int i = 0, column_cnt = 0; i < selectResult.getColumnCount(); i++)
			{
				//cout << "Type is: " << (insertList[i].getType() == mysqlx::Value::RAW;
				if (column_cnt > 0)
					insertColumns << ", ";
				insertColumns << "\"" << insertColumn[i] << "\"";
				column_cnt++;
			}
			cout << "insert into " << table.getName() << "(" << columns << ")" << " values (" << insertColumns.str()
				<< ");" << endl;
			insertColumns.str("");
		}
		return sqlQuery.str();
	}

	std::vector< std::string> dbReplicator::getColumnNames(mysqlx::Table table, bool is_conv)
	{
	
		auto columns = this->getTableMetaData(table);
		stringstream result;
		int column_cnt = 0;
		std::vector< std::string> out;
		for (const auto &column : columns)
		{
			if (column_cnt > 0)
			{
				result.str(", "); ;
			}
			if (is_conv)
			{
				std::string convert_to = "CHAR";
				if (column.second == "geometry")
				{
					result << "ST_AsText(" << column.first << ")";
					out.push_back(result.str());
				}
				else
				{
					if (column.second == "blob")
					{
						;
					}
					else
					{
						;
					}
					result << "convert(" << column.first << "," << convert_to << ")";
					out.push_back(result.str());
				}
			}
			else
			{
				result << column.first;
				out.push_back(result.str());
			}
			
			column_cnt++;
		}
		return out;
	}

	std::string dbReplicator::replicate()
	{
		vector<mysqlx::Table> masterTabList = master_db->getTables();
		vector<mysqlx::Table> slaveTabList = slave_db->getTables();
		vector<mysqlx::Table> new_tables;
		int masterTable = 0, slaveTable = 0;
		bool is_view = false;
		// Главный цикл репликации, обходящий таблицы master и slave
		while (masterTable < masterTabList.size() && slaveTable < slaveTabList.size()) {
			string tmp = masterTabList[masterTable].getName();
			cout << "-- Table: " << tmp << endl;
			if (masterTabList[masterTable].isView()) {
				is_view = true;
				masterTable++;
			}
			if (slaveTabList[slaveTable].isView()) {
				is_view = true;
				slaveTable++;
			}
			if (!is_view) {
				// IF обрабатывает отсутствующие таблицы в slave, находящиеся
				// в master в алфавитном порядке раньше, чем первая в slave
				if (comp_tables(masterTabList[masterTable], slaveTabList[slaveTable])) {
					//new_tables.push_back(masterTabList[i++]);
					cout << createTable(masterTabList[masterTable]);
					cout << insertRows(masterTabList[masterTable], true);
					masterTable++;
				}
				else {
					if (!comp_tables(slaveTabList[slaveTable], masterTabList[masterTable])) { 
						// Таблицы присутствуют в обоих базах, нужно вычислить
						// расхождение и выдать скрипт
						processTables(masterTabList[masterTable], slaveTabList[slaveTable]);
						++masterTable; 
					}
					else {
						cout << createTable(masterTabList[masterTable++]); 
					}
					// Следующая slave таблица
					++slaveTable;
				}
			}
			is_view = false;
		}
		// Отсутствующие таблицы в slave, находящиеся
		// в master в алфавитном порядке позже, чем последняя в slave
		for ( ; masterTable < masterTabList.size(); masterTable++) {
			string tmp = masterTabList[masterTable].getName();
			if (!masterTabList[masterTable].isView())	{
				cout << createTable(masterTabList[masterTable]);
				cout << insertRows(masterTabList[masterTable], true);
			}
		}
		return string();
	}

	bool comp_tables(mysqlx::Table table1, mysqlx::Table table2)
	{
		bool res;
		res = (table1.getName() < table2.getName());
		return res;
	}

	bool comp_columns(const mysqlx::Column &column1, const mysqlx::Column &column2)
	{
		bool res;
		res = (column1.getColumnName() < column2.getColumnName());
		return res;
	}
	
}