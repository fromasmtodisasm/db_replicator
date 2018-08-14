#include "db_replicator.h"
#include <iostream>
#include <algorithm>
#include <iterator>
#include <list>
#include "Config.h"
using namespace std;

namespace replication {

	db_replicator::db_replicator(Params params) : conf(params)
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

	db_replicator::~db_replicator()
	{

	}

	std::map<string,string> db_replicator::get_table_metadata(mysqlx::Table table)
	{
		std::map<string, string> result;
		mysqlx::RowResult rowResult = master_session->sql(
			"SELECT DATA_TYPE, COLUMN_NAME "
			"FROM INFORMATION_SCHEMA.COLUMNS "
			"WHERE "
			"TABLE_SCHEMA = '" + conf["db_from"]["database"] + "' and TABLE_NAME = \"" + (string)table.getName() + "\""
		).execute();

		auto columnList = rowResult.fetchAll();
		for (const auto &row : columnList)
		{
			result[row[1]] = row[0];
		}

		return result;
	}

	std::string db_replicator::create_table(mysqlx::Table table)
	{
		auto columns = get_table_metadata(table);
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
			//sqlQuery << "\t";
			sqlQuery << column.first << " " << column.second;
			column_cnt++;
		}
		sqlQuery << endl << ");" << endl;
		return sqlQuery.str();// sqlQuery.str();

	}

	void db_replicator::process_table(mysqlx::Table table)
	{
	}

	std::string db_replicator::insert_table(mysqlx::Table table)
	{
		stringstream sqlQuery;
		stringstream selectQuery;
		stringstream insertColumns;
		string columns = get_columns_name(table, true);
		
		selectQuery << "select " << columns << " from " << master_session->getDefaultSchemaName()
			<< "." << table.getName() << ";";
		cout << selectQuery.str() << endl;
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
			cout << "insert into " << table.getName() << "(" << get_columns_name(table, false) << ")" << " values (" << insertColumns.str()
				<< ");" << endl;
			insertColumns.str("");
		}
		return sqlQuery.str();
	}

	std::string db_replicator::get_columns_name(mysqlx::Table table, bool is_conv)
	{
	
		auto columns = this->get_table_metadata(table);
		stringstream result;
		int column_cnt = 0;
		for (const auto &column : columns)
		{
			if (column_cnt > 0)
			{
				result << "," << " ";
			}
			if (is_conv)
			{
				std::string convert_to = "CHAR";
				if (column.second == "geometry")
				{
					result << "ST_AsText(" << column.first << ")";
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
				}
			}
			else
			{
				result << column.first;
			}
			
			column_cnt++;
		}
		return result.str();
	}


	std::string db_replicator::replicate()
	{
		vector<mysqlx::Table> masterTabList = master_db->getTables();
		vector<mysqlx::Table> slaveTabList = slave_db->getTables();
		vector<mysqlx::Table> new_tables;
		int i = 0, j = 0;
		bool is_view = false;
		// √лавный цикл репликации, обход€щий таблицы master и slave
		while (i < masterTabList.size() && j < slaveTabList.size()) {
			string tmp = masterTabList[i].getName();
			cout << "Table: " << tmp << endl;
			//if (tmp == std::string("address") || tmp == std::string("stuff"))
			//{
			//	i++;
			//	continue;
			//}
			if (masterTabList[i].isView()) {
				is_view = true;
				i++;
			}
			if (slaveTabList[j].isView()) {
				is_view = true;
				j++;
			}
			if (!is_view) {
				// IF обрабатывает отсутствующие таблицы в slave, наход€щиес€
				// в master в алфавитном пор€дке раньше, чем перва€ в slave
				if (comp_tables(masterTabList[i], slaveTabList[j])) {
					//new_tables.push_back(masterTabList[i++]);
					cout << create_table(masterTabList[i]);
					cout << insert_table(masterTabList[i]);
					i++;
				}
				else {
					if (!comp_tables(slaveTabList[j], masterTabList[i])) { ++i; }
					else {cout << create_table(masterTabList[i++]); }
					++j;
				}
			}
			is_view = false;
		}
		// ќтсутствующие таблицы в slave, наход€щиес€
		// в master в алфавитном пор€дке позже, чем последн€€ в slave
		for ( ; i < masterTabList.size(); i++) {
			string tmp = masterTabList[i].getName();
			if (tmp == std::string("staff"))
			{
				continue;// cout << "here" << endl;
			}
			if (!masterTabList[i].isView())	{
				cout << create_table(masterTabList[i]);
				cout << insert_table(masterTabList[i]);
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
	
}