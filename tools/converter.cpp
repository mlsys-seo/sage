#include "graph.hpp"
#include <boost/program_options.hpp>
#include <iostream>
#include "timer.hpp"

using namespace boost;
using namespace boost::program_options;
namespace po = boost::program_options;

bool process_command_line(int argc, char** argv,
                          std::string& path_text,
                          std::string& path_output,
                          int& type,
                          bool& undirected,
                          bool& is_print) {
	po::options_description desc("Program Usage", 2048, 1024);
	try
	{
		desc.add_options()
				("help",	"produce help message")
				("text,t",	po::value<std::string>(&path_text)->required(),			"Path of graph text file (Required)")
				("output_file,o",	po::value<std::string>(&path_output)->required(),	"Path of output file (Required)")
				("type,T",	po::value<int>(&type)->default_value(AUTO_DETECTED),	"Graph type (Default: Auto)")
				("undirected,u",													"Undirected Graph")
				("print,p",															"Print the content f graph (Default: Auto Detected)\n"
				 																	"\t0: Edge Only\n"
																					"\t1: Edge with only probability\n"
																					"\t2: Edge with probability and attribute")
				;

		po::variables_map vm;
		po::store(po::parse_command_line(argc, argv, desc), vm);
		if (vm.count("help"))
		{
			std::cout << desc << "\n";
			return false;
		}

		if (vm.count("print"))
			is_print = true;
		else
			is_print = false;

		if (vm.count("undirected"))
			undirected = true;
		else
			undirected = false;

		po::notify(vm);

		if (type > AUTO_DETECTED || type < EDGE_ONLY){
			std::cerr << "Input type between range\n";
			std::cout << desc << "\n";
			return false;
		}
	}
	catch(std::exception& e)
	{
		std::cerr << "Error: " << e.what() << "\n";
		std::cout << desc << "\n";
		return false;
	}
	catch(...)
	{
		std::cerr << "Unknown error!" << "\n";
		std::cout << desc << "\n";
		return false;
	}

	return true;
}

int main(int argc, char** argv) {
	std::string path_txt;
	std::string path_output;
	int type;
	bool is_print;
	bool undirected;

	bool result = process_command_line(argc, argv, path_txt, path_output, type, undirected, is_print);
	if (!result)
		return -1;

	struct timeval start, end;
	gettimeofday(&start, 0);

	Graph g;
	g.LoadText(path_txt.c_str(), undirected, type);
	g.Save(path_output.c_str());

	gettimeofday(&end, 0);

	printf("type: %d\n", g.GetType());
	printf("undirected: %d\n", g.GetUnidrected());
	printf("max vid: %d\n", g.GetMaxVertexId());
	printf("num edges: %lu\n", g.GetNumEdges());
	printf("elapsed time: %f\n", TimeDiff(start, end));

	if(is_print) {
		g.PrintEdgeList();
		g.PrintIndex(true);
		g.PrintIndex(false);
	}

	return 0;
};
