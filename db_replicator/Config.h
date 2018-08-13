#pragma once
#include <map>
#include <string>
namespace replication {
	typedef std::map<std::string, std::map<std::string, std::string>> Params;

	class Config
	{
	public:
		Config(std::string file);
		~Config();
		bool getParams(Params &conf);
	private:
		std::string file;
		Params *conf;
	};

}