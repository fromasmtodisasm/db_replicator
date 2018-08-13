#include "Config.h"
#include "INIParser.h"
#include <iostream>

namespace replication {

	Config::Config(std::string file) : file(file)
	{
		
	}


	Config::~Config()
	{
		
	}

	bool Config::getParams(Params &conf)
	{
		INIReader reader(file);
		std::string params[] = { "user", "pwd", "host", "port", "database" };
		std::string curr_val;
		

		if (reader.ParseError() < 0) {
			std::cout << "Can't load '" << file << "'\n";
			return false;
		}


		for (auto param : params)
		{
			if ((curr_val = reader.Get("db_from", param, "UNKNOWN")) == "UNKNOWN")
			{
				return false;
			}
			conf["db_from"].insert(make_pair(param, curr_val));

			if ((curr_val = reader.Get("db_to", param, "UNKNOWN")) == "UNKNOWN")
			{
				return false;
			}
			conf["db_to"].insert(make_pair(param, curr_val));
		}
		return true;
	}

}