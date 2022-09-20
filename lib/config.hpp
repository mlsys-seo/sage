#ifndef SAGE_CONFIG_HPP
#define SAGE_CONFIG_HPP

#include <boost/program_options.hpp>
#include <iostream>
#include "timer.hpp"

using namespace boost;
using namespace boost::program_options;
namespace po = boost::program_options;

size_t NUM_SAMPLE = 0;
size_t NUM_INIT_ALLOCATED = 2;
int NUM_SCATTER_SECTION = 0;
int NUM_GATHER_SECTION = 0;
unsigned int SEED = 0;
bool INIT_ALL_VALUE = 0;
size_t STAT_MAX_NUM_VALUE = 0;
size_t STAT_MAX_VERTEX_SIZE = 0;
size_t PAGE_SIZE = 512UL;

#ifndef VERSION
#define VERSION 0
#endif

class Config{
	char *bin;
	std::string file_path;
	size_t num_thread;
	size_t vertex_partition_size;
	size_t num_sample;
	size_t page_size;
	unsigned int sample_process_mode;
	int num_scatter_section;
	int num_gather_section;

	bool is_certain_graph;
	float batch_sampling_ratio;
	unsigned int seed;

	bool has_edge_attr;
	bool use_sampling_vertex_activation;
	std::string option;

	float cache_memory_mb;
	unsigned int num_allocated_value;
	bool init_all_value;

public:
	void Print(){
		std::cout << "version: " << VERSION << "\n";
		std::cout << "bin: " << bin << "\n";
		std::cout << "date: " << CurrentTime() << "\n";
		std::cout << "file_path: " << file_path << "\n";

		std::cout << "num_thread: " << num_thread << "\n";
		std::cout << "vertex_partition_size: " << vertex_partition_size << "\n";
		std::cout << "num_sample: " << num_sample << "\n";
//		std::cout << "num_scatter_section: " << num_scatter_section << "\n";
		std::cout << "num_scatter_section: " << NUM_SCATTER_SECTION << "\n";
//		std::cout << "num_gather_section: " << num_gather_section << "\n";
		std::cout << "num_gather_section: " << NUM_GATHER_SECTION << "\n";
		std::cout << "sample_process_mode: " << sample_process_mode << "\n";
		std::cout << "cache_memory_mb: " << cache_memory_mb << "\n";
		std::cout << "page_size: " << page_size << "\n";

		std::cout << "is_certain_graph: " << is_certain_graph << "\n";

		std::cout << "batch_sampling_ratio: " << batch_sampling_ratio << "\n";
		std::cout << "seed: " << seed << "\n";
		std::cout << "has_edge_attr: " << has_edge_attr << "\n";
		std::cout << "use_sampling_vertex_activation: " << use_sampling_vertex_activation << "\n";

		std::cout << "option: " << option << "\n";
		std::cout << "\n";
	}

	Config(int argc, char** argv) {
		bin = argv[0];
		po::options_description desc("Program Usage", 2048, 1024);
		try
		{
			desc.add_options()
					("help",	"produce help message")
					("file_path,f",	po::value<std::string>(&file_path)->required(),		"Path of graph file (Required)")
					("num_thread,t",	po::value<size_t>(&num_thread)->default_value(8),		"Number of threads (Default: 8)")
					("num_allocated_value,v",	po::value<unsigned int>(&num_allocated_value)->default_value(2),		"Number of allocated value (Default: 2)")
					("sample_process_mode,m",	po::value<unsigned int>(&sample_process_mode)->default_value(3),		"Sample Process Mode (Default: 3)")
					("init_all_value,I",	po::value<bool>(&init_all_value)->default_value(false),		"Init all value (Default: 0)")
					("cache_memory_mb,c",	po::value<float>(&cache_memory_mb)->default_value(4096),		"Cache Memory Size (MB, Default: 4096)")
//					("num_scatter_section,S",	po::value<int>(&num_scatter_section)->default_value(0),		"Number of scatter section (Default: 0)")
					("num_gather_section,G",	po::value<int>(&num_gather_section)->default_value(1000),		"Number of gather section (Default: 1000)")
					("num_sample,s",	po::value<size_t>(&num_sample)->default_value(1000),		"Number of sampels (Default: 1000)")
					("certain_graph,C",	po::value<bool>(&is_certain_graph)->default_value(false), "Certain Graph Algorithm (Default: False)")
					("vertex_partition_size,p",	po::value<size_t>(&vertex_partition_size)->default_value(1),		"Size of vertex partition (Default: 1)")
					("page_size,P",	po::value<size_t>(&page_size)->default_value(512),		"Page size (Default: 512)")
					("seed,e",	po::value<unsigned int>(&seed)->default_value(0),		"random seed (Default:0)")
					("batch_sampling_ratio,b",	po::value<float>(&batch_sampling_ratio)->default_value(0),		"Ratio of batch sampling (Default: 0)")
					("option,o",	po::value<std::string>(&option),		"Option of algorithms")
					;

			po::variables_map vm;
			po::store(po::parse_command_line(argc, argv, desc), vm);

			if (vm.count("help"))
			{
				std::cout << desc << "\n";
				return;
			}
			po::notify(vm);

			switch(sample_process_mode){
				case 0:
				case 1:
				case 2:
                case 3:
					break;
				default:
					std::cerr << "Error: incorrect sample process mode (0~2)\n";
					exit(-1);
			}

			if (is_certain_graph) {
				batch_sampling_ratio = 0;
			}

			if(is_certain_graph ||  sample_process_mode == 0)
				use_sampling_vertex_activation = false;
			else
				use_sampling_vertex_activation = true;
			has_edge_attr = false;

			PAGE_SIZE = page_size;

			if(num_gather_section)
				num_scatter_section = 1;
			else
				num_scatter_section = 0;
			NUM_SCATTER_SECTION = num_scatter_section;
			NUM_GATHER_SECTION = num_gather_section;

			NUM_SAMPLE = num_sample;
			INIT_ALL_VALUE = init_all_value;
			SEED = seed;
			NUM_INIT_ALLOCATED = num_allocated_value;
			if(num_allocated_value == 0)
				NUM_INIT_ALLOCATED = NUM_SAMPLE+1;

		}
		catch(std::exception& e)
		{
			std::cerr << "Error: " << e.what() << "\n";
			std::cout << desc << "\n";
			exit(-1);
		}
		catch(...)
		{
			std::cerr << "Unknown error!" << "\n";
			exit(-1);
		}
	}

	inline std::string &GetFilePath() {
		return file_path;
	}

	inline std::size_t GetNumThread() {
		return num_thread;
	}

	inline unsigned int GetSeed(){
		return seed;
	}

	inline std::size_t GetVertexPartitionSize() {
		return vertex_partition_size;
	}

	inline std::size_t GetNumSample() {
		return num_sample;
	}

	inline bool GetCertainGraph() {
		return is_certain_graph;
	}

	inline void SetCertainGraph(bool is_certain_graph) {
		this->is_certain_graph = is_certain_graph;
	}

	inline float GetCacheMemoryMB(){
		return cache_memory_mb;
	}

	inline bool GetHasEdgeAttribute() {
		return has_edge_attr;
	}

	inline void SetHasEdgeAttribute(bool has_edge_attr) {
		this->has_edge_attr = has_edge_attr;
	}

	inline float GetBatchSamplingRatio() {
		return batch_sampling_ratio;
	}

	inline void SetUseSamplingVertexActivation(bool use_sampling_vertex_activation) {
		this->use_sampling_vertex_activation = use_sampling_vertex_activation;
	}

	inline bool GetUseSamplingVertexActivation(){
		return use_sampling_vertex_activation;
	}

	inline unsigned int GetSampleProcessMode(){
		return sample_process_mode;
	}

	inline std::string GetOption(){
		return option;
	}
};

#endif //SAGE_CONFIG_HPP
