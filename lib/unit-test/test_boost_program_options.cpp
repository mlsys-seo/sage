#include <boost/program_options.hpp>
#include <iostream>

using namespace boost;
using namespace boost::program_options;
namespace po = boost::program_options;

#define MAX_TYPE 2
#define MIN_TYPE 0

bool process_command_line(int argc, char** argv,
                          std::string& path_text,
                          std::string& path_csr,
                          std::string& path_idx,
                          int& type,
                          bool& is_print) {
	po::options_description desc("Program Usage", 2048, 1024);
	try
	{
		desc.add_options()
				("help",	"produce help message")
				("text,t",	po::value<std::string>(&path_text)->required(),		"Path of graph text file (Required)")
				("csr,c",	po::value<std::string>(&path_csr)->required(),		"Path of graph CSR file (Required)")
				("index,i",	po::value<std::string>(&path_idx)->required(),		"Path of graph Index file (Required)")
				("type,T",	po::value<int>(&type)->default_value(MAX_TYPE),		"Graph type (Default: Auto)")
				("print,p",														"Print the content of graph (Default: Auto Detected)\n"
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

		po::notify(vm);

		if (type < MIN_TYPE || type > MAX_TYPE){
			std::cerr << "Input type between (0-3)\n";
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
		return false;
	}

	return true;
}

int main(int argc, char** argv) {
	std::string path_txt;
	std::string path_csr;
	std::string path_idx;
	int type;
	bool is_print;

	bool result = process_command_line(argc, argv, path_txt, path_csr, path_idx, type, is_print);
	if (!result)
		return -1;

	std::cout << "type: " << type << "\n";
	std::cout << "path_txt: " << path_txt << "\n";
	std::cout << "path_csr: " << path_csr << "\n";
	std::cout << "path_idx: " << path_idx << "\n";
	std::cout << "is_print: " << is_print << "\n";

	return 0;
};
