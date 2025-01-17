#include "db_replicator.h"
#include "Config.h"

#include <cstdlib>
#include <iostream>
#include <fstream>
#include <map>


using std::string;
using std::cout;
using std::cin;
using std::endl;

using namespace replication;

int main()
{
	
	try
	{
		Config *conf;
		try
		{
			conf = new Config("reply.ini");
		}
		catch (std::exception &e)
		{
			std::cerr << e.what() << endl;
			return EXIT_FAILURE;
		}
		Params params;
		if (conf->getParams(params))
		{
			dbReplicator replicator(params);
			replicator.replicate();
		}
		
	}
	catch (std::exception &e)
	{
		std::cerr << e.what() << endl;
		return EXIT_FAILURE;
	}
	
	//std::system("pause");
    return 0;
}

