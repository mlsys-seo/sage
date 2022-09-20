#ifndef SAGE_GRAPH_ENGINE_HPP
#define SAGE_GRAPH_ENGINE_HPP

#include "graph.hpp"
#include "allocator.hpp"
#include "vertex_program_iterator_thread.hpp"
#include "bitmap.hpp"
#include "vertex_program.hpp"
#include "query_thread.hpp"
#include "timer.hpp"
#include "config.hpp"
#include "sampling_thread.hpp"
#include <sys/time.h>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <algorithm>
#include <cassert>
#include <mutex>
#include "serialized_value_bitmap.hpp"
#include "vertex_cache_aio.hpp"
#include <vector>

namespace logging = boost::log;
namespace src = boost::log::sources;
namespace keywords = boost::log::keywords;

template <typename VertexType>
class GraphEngine{
	Bitmap* ptr_activated_vertex;
	BitmapArray activated_vertex_bitmap_array[2];
	Graph graph;
	unsigned int current_iteration;
	void* ptr_vertex_array;
	Bit64** sampling_index;
	Config& config;
	size_t num_threads;
	struct timeval engine_start, engine_end;
	std::mutex* mtx_array;
	unsigned int sample_process_mode;
	size_t num_sample;
	size_t num_run;
	VertexProgramIteratorThread<VertexType>** vertex_program_iterator_threads;
	Bitmap all_true_bitmap;
	DiskOffsetAllocator disk_offset_allocator;
	int fd;
	void* init_value_page;
	VertexCache* ptr_vertex_cache;
	VertexCacheAio* ptr_vertex_cache_aio;
	bool start_all;
	pthread_spinlock_t memory_alloc_lock;
	size_t memory_usage;
	size_t max_memory_usage;

	std::set<VertexId> running_set;
	pthread_spinlock_t lock;
	size_t max_dest;
	size_t min_dest;
	std::string filename;
	size_t total_program_sum;
	size_t total_program_cnt;
	size_t cnt_vertex_type0;
	size_t cnt_vertex_type1;
	size_t cnt_value;
	size_t cnt_transform;
    size_t cnt_query;
    size_t cnt_program;

public:
    bool call_start_func;

    GraphEngine(){

    }

	void AddRunningSet(std::vector<VertexId> vertices, bool scatter){
		pthread_spin_lock(&lock);
		for(int i=0; i<vertices.size(); i++)
			running_set.insert(vertices[i]);

		VertexId min_vertex = *running_set.begin();
		auto current_section = GetCacheSection(scatter, min_vertex);
		ptr_vertex_cache->SetCurrentSection(current_section.first);
		pthread_spin_unlock(&lock);
	}

	void RemoveRunningSet(VertexId vertex){
		pthread_spin_lock(&lock);
		running_set.erase(vertex);
		pthread_spin_unlock(&lock);
	}

	void PrintMemoryUsage(){
		printf("memory usage: %lu / %lu\n", memory_usage, max_memory_usage);
	}

	GraphEngine(Config& config)
		: config(config)
		, sampling_index(NULL)
		, ptr_vertex_array(NULL)
		, current_iteration(0){

		logging::add_console_log(std::cout);

		gettimeofday(&engine_start, 0);



		pthread_spin_init(&lock, 0);
		pthread_spin_init(&memory_alloc_lock, 0 );
		memory_usage = 0;
		max_memory_usage = 0;
		total_program_sum = 0;
		total_program_cnt = 0;

        cnt_vertex_type0 = 0;
        cnt_vertex_type1 = 0;
        cnt_value = 0;
        cnt_transform = 0;

        cnt_query = 0;
        cnt_program = 0;

		struct timeval start, end;
		gettimeofday(&start, 0);
		graph.Load(config.GetFilePath().c_str());

		if(NUM_SCATTER_SECTION < 0)
			NUM_SCATTER_SECTION = graph.GetMaxVertexId() / -NUM_SCATTER_SECTION;
		if(NUM_GATHER_SECTION < 0)
			NUM_GATHER_SECTION  = graph.GetMaxVertexId() / -NUM_GATHER_SECTION;

		config.Print();

		gettimeofday(&end, 0);
		BOOST_LOG_TRIVIAL(info)  << "loading time: " <<  TimeDiff(start, end);



		/* PAGE_SIZE 조정
		if(!PAGE_SIZE){
			PAGE_SIZE = 512;
			size_t vertex_size;
			if(config.GetSampleProcessMode() == 1)
		}
		 */

		SerializedSimVertex<VertexType>::PrintSize(NUM_SAMPLE);
		SerializedBundleVertex<VertexType>::PrintSize(NUM_SAMPLE);
		fd = NULL;
		num_threads = config.GetNumThread();
		sample_process_mode = config.GetSampleProcessMode();
		num_sample = config.GetNumSample();
		num_run = 0;
		current_iteration = 0;

		if(config.GetCertainGraph()){
			all_true_bitmap.Init(num_sample);
			all_true_bitmap.SetAll();
		}

		ptr_activated_vertex = new Bitmap(graph.GetMaxVertexId()+1);

		filename = "sage.tmp";
		remove(filename.c_str());
		std::cout << filename << "\n";
		fd = open(filename.c_str(), O_RDWR| O_DIRECT | O_CREAT, 0666);
		if (fd < 0) {
			fprintf(stderr, "not exist %d\n", fd);
			exit(-1);
		}

		size_t alloc_unit_size;
		float total_vertex_size = (SerializedBundleVertex<VertexType>::GetAllocPageUnit(num_sample) * PAGE_SIZE * (size_t) (graph.GetMaxVertexId() + 1UL) ) / 1024.0 / 1024;
		if(sample_process_mode == 1)
			alloc_unit_size = SerializedBundleVertex<VertexType>::GetAllocPageUnit(num_sample) * PAGE_SIZE;
		else if(sample_process_mode == 2)
			alloc_unit_size = SerializedSimVertex<VertexType>::GetAllocPageUnit(num_sample) * PAGE_SIZE;
        else if(sample_process_mode == 3)
            alloc_unit_size = SerializedSimVertex<VertexType>::GetAllocPageUnit(num_sample) * PAGE_SIZE;

		printf("total vertex size (MB): %.1f\n", total_vertex_size);

		posix_memalign((void **) &init_value_page, PAGE_SIZE, alloc_unit_size);
		memset(init_value_page, 0x00, alloc_unit_size);
		*((VertexId*) init_value_page) = ~0U;

		if(config.GetSampleProcessMode() == 1) {
			SerializedBundleVertex<VertexType> init_vertex(~0U, num_sample, init_value_page);
			for(int i=0; i<NUM_SAMPLE;i++) {
				VertexType *v = init_vertex.GetVertexAttributeByIndex(i);
				v->Init();
			}
			init_vertex.GetActivationBitmap(0).ClearAll();
			init_vertex.GetActivationBitmap(1).ClearAll();
		}else if(config.GetSampleProcessMode() == 2) {
			SerializedSimVertex<VertexType> init_vertex(~0U, num_sample, &init_value_page);
			std::pair<VertexType*, Bitmap> entry = init_vertex.AllocNewValue(NULL);
			VertexType v;
			v.Init();
			*(entry.first) = v;
			entry.second.SetAll();
			assert(init_vertex.GetNumValue() == 1);
			init_vertex.GetActivationBitmap(0).ClearAll();
			init_vertex.GetActivationBitmap(1).ClearAll();
        }else if(config.GetSampleProcessMode() == 3) {
            SerializedSimVertex<VertexType> init_vertex(~0U, num_sample, &init_value_page);
            std::pair<VertexType*, Bitmap> entry = init_vertex.AllocNewValue(NULL);
            VertexType v;
            v.Init();
            *(entry.first) = v;
            entry.second.SetAll();
            assert(init_vertex.GetNumValue() == 1);
            init_vertex.GetActivationBitmap(0).ClearAll();
            init_vertex.GetActivationBitmap(1).ClearAll();
		}

		//Cache Init
		size_t sampling_memory = 0;
		if(config.GetBatchSamplingRatio())
			sampling_memory = (graph.GetNumEdges()*sizeof(Bit64*) + graph.GetNumEdges()*((size_t)config.GetBatchSamplingRatio())*((NUM_SAMPLE+63UL)/64UL)*8UL)/2UL/1024UL/1024UL;
		float cache_memory = config.GetCacheMemoryMB();
		if(cache_memory < 0)
			cache_memory = (float)total_vertex_size * (-cache_memory);

		printf("sampling_memory (MB): %lu\n", sampling_memory);

		ptr_vertex_cache = new VertexCache(fd, cache_memory*1024UL*1024UL, graph.GetMaxVertexId()+1, alloc_unit_size, init_value_page, NUM_SCATTER_SECTION+NUM_GATHER_SECTION+2);
		ptr_vertex_cache_aio = new VertexCacheAio(ptr_vertex_cache, config.GetVertexPartitionSize());

		vertex_program_iterator_threads = new VertexProgramIteratorThread<VertexType>*[num_threads];
		for (int i = 0; i < num_threads; i++)
			vertex_program_iterator_threads[i] = new VertexProgramIteratorThread<VertexType>(i, this, &graph,
			                                                                                 fd,
			                                                                                 NULL,
			                                                                                 ptr_vertex_array, NULL,
			                                                                                 ptr_vertex_cache);

		start_all = false;
        call_start_func = true;
        //InitVertex();
	}

	~GraphEngine(){
		gettimeofday(&engine_end, 0);
		ptr_vertex_cache->PrintStat(0);
		BOOST_LOG_TRIVIAL(info)  << "avg gather: " <<  (float) total_program_sum / total_program_cnt << "\n";
		BOOST_LOG_TRIVIAL(info)  << "collective gather: " <<  total_program_cnt << "\n";
		BOOST_LOG_TRIVIAL(info)  << "sample gather: " <<  total_program_sum << "\n";
		BOOST_LOG_TRIVIAL(info)  << "cnt type0: " <<  cnt_vertex_type0 << "\n";
        BOOST_LOG_TRIVIAL(info) << "cnt type1: " << cnt_vertex_type1 << "\n";
        BOOST_LOG_TRIVIAL(info)  << "cnt value: " <<  cnt_value << "\n";
        BOOST_LOG_TRIVIAL(info)  << "cnt query: " <<  cnt_query << "\n";
        BOOST_LOG_TRIVIAL(info)  << "cnt program: " <<  cnt_program << "\n";
        BOOST_LOG_TRIVIAL(info)  << "cnt transform: " <<  cnt_transform << "\n";
        BOOST_LOG_TRIVIAL(info)  << "engine time: " <<  TimeDiff(engine_start, engine_end);


		delete(ptr_activated_vertex);

		delete ptr_vertex_cache_aio;
		delete ptr_vertex_cache;

		for (int i = 0; i < num_threads; i++)
			delete vertex_program_iterator_threads[i];

		delete vertex_program_iterator_threads;

		if(sampling_index){
			for(int i=0; i<graph.GetNumEdges(); i++) {
				if (sampling_index[i])
					delete sampling_index[i];
			}
			delete[] sampling_index;
		}
		//remove(filename.c_str());
	}

	void SumStat(size_t cnt_query, size_t cnt_vertex_type0, size_t cnt_vertex_type1, size_t cnt_value, size_t cnt_program, size_t cnt_transform){
        pthread_spin_lock(&lock);
        this->cnt_query += cnt_query;
	    this->cnt_vertex_type0 += cnt_vertex_type0;
        this->cnt_vertex_type1 += cnt_vertex_type1;
        this->cnt_value += cnt_value;
        this->cnt_program += cnt_program;
        this->cnt_transform += cnt_transform;
        pthread_spin_unlock(&lock);
	}
	DiskOffsetAllocator* GetDiskOffsetAllocatorPtr(){
		return &disk_offset_allocator;
	}

	void* GetInitValuePage(){
		return init_value_page;
	}

	void InitVertex() {
		struct timeval start, end;
		gettimeofday(&start, 0);

		current_iteration = 0;
		ptr_vertex_cache->Init();

		Bitmap all_vertex(graph.GetMaxVertexId() + 1);
		all_vertex.SetAll();
		VertexAllocator init_allocator(&all_vertex);
		for (int i = 0; i < num_threads; i++)
			vertex_program_iterator_threads[i]->SetVertexAllocator(&init_allocator);
		for (int i = 0; i < num_threads; i++)
			vertex_program_iterator_threads[i]->SetStep(INIT);
		for (int i = 0; i < num_threads; i++)
			vertex_program_iterator_threads[i]->Start();
		for (int i = 0; i < num_threads; i++)
			vertex_program_iterator_threads[i]->Join();
		gettimeofday(&end, 0);
		BOOST_LOG_TRIVIAL(info)  << "init time: " <<  TimeDiff(start, end);
	}

	inline Bitmap& GetAllTrueBitmap(){
		return all_true_bitmap;
	}

	std::pair<int, double> GetCacheSection(bool is_scatter, VertexId vertex){
		if(NUM_SCATTER_SECTION + NUM_GATHER_SECTION == 0)
			return {0, 0};
		if(NUM_SCATTER_SECTION + NUM_GATHER_SECTION == 1)
			return {2, 0};
		int scatter_section_size = (graph.GetMaxVertexId()+NUM_SCATTER_SECTION) / NUM_SCATTER_SECTION;
		int gather_section_size = (graph.GetMaxVertexId()+NUM_GATHER_SECTION) / NUM_GATHER_SECTION;

		double priority;
		if(is_scatter)
			priority = graph.GetMaxVertexId()*(current_iteration*2 + 1) + vertex;
		else
			priority = graph.GetMaxVertexId()*current_iteration*2 + vertex;

		int section;
		if(is_scatter)
			section = 2 + NUM_GATHER_SECTION + vertex/scatter_section_size;
		else
			section = 2 + vertex/gather_section_size;
		return {section, priority};
	}

	inline std::mutex& LockVertex(VertexId v){
		mtx_array[v].lock();
	}

	inline std::mutex& UnlockVertex(VertexId v){
		mtx_array[v].unlock();
	}

	Graph* GetGraph(){
		return &graph;
	}

	Config& GetConfig(){
		return config;
	}

	VertexCache* GetVertexCache(){
		return ptr_vertex_cache;
	}

	inline BitmapArray* GetCurrentActivatedVertexBitmapArray(){
		return &activated_vertex_bitmap_array[current_iteration%2];
	}

	inline BitmapArray* GetNextActivatedVertexBitmapArray(){
		return &activated_vertex_bitmap_array[(current_iteration+1)%2];
	}

	inline void* GetVertexArrayPtr(){
		return ptr_vertex_array;
	}

	inline size_t GetNumRun(){
		return num_run;
	}

	unsigned int GetCurrentIteration(){
		return current_iteration;
	}

	inline Bit64** GetSamplingIndex(){
		return sampling_index;
	}

	void ActivateAll(){
		ptr_activated_vertex->SetAll();
		if(config.GetUseSamplingVertexActivation()) {
			start_all = true;
			if(config.GetSampleProcessMode() == 1) {
				SerializedBundleVertex<VertexType> init_vertex(~0U, num_sample, init_value_page);
				init_vertex.GetActivationBitmap(1).SetAll();
			}else if(config.GetSampleProcessMode() == 2) {
				SerializedSimVertex<VertexType> init_vertex(~0U, num_sample, &init_value_page);
				init_vertex.GetActivationBitmap(1).SetAll();
            }else if(config.GetSampleProcessMode() == 3) {
                SerializedSimVertex<VertexType> init_vertex(~0U, num_sample, &init_value_page);
                init_vertex.GetActivationBitmap(1).SetAll();
			}
		}
	}

	void ActivateVertex(VertexId vertex_id, Bitmap* activated_dest_bitmap){
		ptr_activated_vertex->Set(vertex_id);
		if(config.GetUseSamplingVertexActivation()){
			activated_dest_bitmap->SetAll();

			for(int i=0; i<NUM_SAMPLE; i++) {
				assert(activated_dest_bitmap->Get(i));
			}
			start_all = false;
		}
	}

	void SetStartVertex(VertexId vertex_id){
        ptr_activated_vertex->Set(vertex_id);

//		ptr_vertex_cache_aio->ReadAppend(vertex_id, NULL);
//		ptr_vertex_cache_aio->SubmitRequests();
//		while(ptr_vertex_cache_aio->GetNumWaitingRequests()){
//			std::vector<VertexRequest> src_events = ptr_vertex_cache_aio->WaitForComplete(1);
//
//			for (int j = 0; j < src_events.size(); j++) {
//				VertexId src = src_events[j].vid;
//				void *read_buf = src_events[j].buf;
//				size_t buf_size = src_events[j].size;
//				void* write_buf = NULL;
//
//				posix_memalign(&write_buf, PAGE_SIZE, buf_size);
//				memcpy(write_buf, read_buf, buf_size);
//				ptr_vertex_cache_aio->EndRead(vertex_id, read_buf);
//				*((VertexId*) write_buf) = vertex_id;
//
//				VertexId _vertex_id = *((VertexId *) write_buf);
//				assert(vertex_id == _vertex_id || _vertex_id == ~0U);
//
//                _SetStartVertex(vertex_id, read_buf, write_buf, call_start_func);
//
//				ptr_vertex_cache_aio->Write(vertex_id, write_buf, buf_size, 0, 0);
//			}
//		}

    }

    void _SetStartVertex(VertexId vertex_id, void* read_buf, void* write_buf, bool call_start_func = true){
        if(config.GetSampleProcessMode() == 1) {
            SerializedBundleVertex<VertexType> vertex_attr(vertex_id, num_sample, write_buf);
            if(call_start_func) {
                for (int i = 0; i < num_sample; i++) {
                    VertexType *ptr_sample_vertex = vertex_attr.GetVertexAttributeByIndex(i);
                    ptr_sample_vertex->Start();
                }
            }

            Bitmap activate_bitmap = vertex_attr.GetActivationBitmap(1);
            ActivateVertex(vertex_id, &activate_bitmap);
            assert(activate_bitmap.IsAllSet());
        }else if(config.GetSampleProcessMode() == 2){
            SerializedSimVertex<VertexType> vertex_attr(vertex_id, num_sample, &write_buf);

            VertexType start_v;

            if(call_start_func) {
                std::pair<VertexType *, Bitmap> entry = vertex_attr.GetValueBitmapByIndex(0);
                start_v.Start();
                *(entry.first) = start_v;
                entry.second.SetAll();
            }

            Bitmap activate_bitmap = vertex_attr.GetActivationBitmap(1);
            ActivateVertex(vertex_id, &activate_bitmap);
            assert(activate_bitmap.IsAllSet());
        }else if(config.GetSampleProcessMode() == 3){
            SerializedSimVertex<VertexType> vertex_attr(vertex_id, num_sample, &write_buf);

            VertexType start_v;

            if(call_start_func) {
                std::pair<VertexType *, Bitmap> entry = vertex_attr.GetValueBitmapByIndex(0);
                start_v.Start();
                *(entry.first) = start_v;
                entry.second.SetAll();
            }

            Bitmap activate_bitmap = vertex_attr.GetActivationBitmap(1);
            ActivateVertex(vertex_id, &activate_bitmap);
            assert(activate_bitmap.IsAllSet());
        }
    }
	template<class VertexProgramType>
	void Run(bool call_start_func = true) {
		struct timeval total_start, total_end, iter_start, iter_end, program_start, program_end;
		gettimeofday(&total_start, 0);
		VertexProgramType* vertex_program_array = new VertexProgramType[num_threads];
		unsigned int activated_count;
		current_iteration = 0;

		for (int i = 0; i < num_threads; i++)
			vertex_program_iterator_threads[i]->SetVertexProgram(&vertex_program_array[i]);

		do {
			current_iteration++;
			BOOST_LOG_TRIVIAL(trace)  << "\tIter: " << current_iteration;
			gettimeofday(&iter_start, 0);

			for (int i = 0; i < num_threads; i++)
				vertex_program_iterator_threads[i]->Init();

            if(current_iteration == 1) {
                VertexAllocator activated_vertex_allocator2(ptr_activated_vertex);
                gettimeofday(&program_start, 0);
                for (int i = 0; i < num_threads; i++)
                    vertex_program_iterator_threads[i]->SetVertexAllocator(&activated_vertex_allocator2);
                for (int i = 0; i < num_threads; i++)
                    vertex_program_iterator_threads[i]->SetStep(START);
                for (int i = 0; i < num_threads; i++)
                    vertex_program_iterator_threads[i]->Start();
                for (int i = 0; i < num_threads; i++)
                    vertex_program_iterator_threads[i]->Join();

                gettimeofday(&program_end, 0);
                BOOST_LOG_TRIVIAL(trace) << "\tStart Time: " << TimeDiff(program_start, program_end);
            }

			VertexAllocator activated_vertex_allocator(ptr_activated_vertex);

			gettimeofday(&program_start, 0);
			for (int i = 0; i < num_threads; i++)
				vertex_program_iterator_threads[i]->SetVertexAllocator(&activated_vertex_allocator);
			for (int i = 0; i < num_threads; i++)
				vertex_program_iterator_threads[i]->SetStep(SCATTER);
			for (int i = 0; i < num_threads; i++)
				vertex_program_iterator_threads[i]->Start();
			for (int i = 0; i < num_threads; i++)
				vertex_program_iterator_threads[i]->Join();

			gettimeofday(&program_end, 0);
			BOOST_LOG_TRIVIAL(trace)  << "\tScatter Time: " << TimeDiff(program_start, program_end);

			// Merge After Scatter
			Bitmap scattered_vertex = Bitmap(graph.GetMaxVertexId() + 1);
			Bitmap scattered_out_neighbor = Bitmap(graph.GetMaxVertexId() + 1);

			for (int i = 0; i < num_threads; i++)
				scattered_vertex.Or(*(vertex_program_iterator_threads[i]->GetScatteredVertex()));
			for (int i = 0; i < num_threads; i++)
				scattered_out_neighbor.Or(*(vertex_program_iterator_threads[i]->GetScatteredOutNeighbor()));
			for (int i = 0; i < num_threads; i++) {
				vertex_program_iterator_threads[i]->SetMergedScatteredVertex(&scattered_vertex);
				vertex_program_iterator_threads[i]->SetMergedScatteredOutNeighbor(&scattered_out_neighbor);
			}

			// Gather
			gettimeofday(&program_start, 0);
			VertexAllocator gather_by_in_edge_allocator(&scattered_out_neighbor);
			for (int i = 0; i < num_threads; i++)
				vertex_program_iterator_threads[i]->SetVertexAllocator(&gather_by_in_edge_allocator);
			for (int i = 0; i < num_threads; i++)
				vertex_program_iterator_threads[i]->SetStep(GATHER);
			for (int i = 0; i < num_threads; i++)
				vertex_program_iterator_threads[i]->Start();
			for (int i = 0; i < num_threads; i++)
				vertex_program_iterator_threads[i]->Join();
			gettimeofday(&program_end, 0);
			BOOST_LOG_TRIVIAL(trace)  << "\tGather Time: " << TimeDiff(program_start, program_end);

			size_t map_size = 0;
			size_t map_cnt = 0;
			size_t program_sum = 0;
			size_t program_cnt = 0;

			for (int i = 0; i < num_threads; i++) {
				auto it = vertex_program_iterator_threads[i]->GetSumMapSize();
				map_size += it.first;
				map_cnt += it.second;

				auto it2 = vertex_program_iterator_threads[i]->GetProgramIter();
				program_sum += it2.first;
				program_cnt += it2.second;
			}
			ptr_activated_vertex->ClearAll();
			for (int i = 0; i < num_threads; i++)
				ptr_activated_vertex->Or(*(vertex_program_iterator_threads[i]->GetActivatedVertex()));

			float avg_map_size = map_size / (float) map_cnt;
			float avg_program_iter = program_sum / (float) program_cnt;
			total_program_cnt += program_cnt;
			total_program_sum += program_sum;
			activated_count = 0;
			for (int i = 0; i < num_threads; i++)
				activated_count += vertex_program_iterator_threads[i]->GetActivatedCount();

			gettimeofday(&iter_end, 0);
			ptr_vertex_cache->PrintStat(current_iteration);
			BOOST_LOG_TRIVIAL(trace) << "\tmax_num_value: " << STAT_MAX_NUM_VALUE << ", \tmax_vertex_size: " << STAT_MAX_VERTEX_SIZE;
			BOOST_LOG_TRIVIAL(trace)  << "\tactivated count: " << activated_count << ",\t avg_map_size: " << avg_map_size << "\t program_iter: " << avg_program_iter << ",\t elapsed time: " << TimeDiff(iter_start, iter_end) << "\n";

			map_size = 0;
			map_cnt = 0;
		} while(activated_count > 0);
		current_iteration = 0;
		num_run++;

		delete[] vertex_program_array;

		gettimeofday(&total_end, 0);
		BOOST_LOG_TRIVIAL(trace)  << "running time: " << TimeDiff(total_start, total_end);
	}

	Bitmap* SelectVertex(Bitmap* _ptr_sample_vertex){
		struct timeval start, end;
		Bitmap* res = _ptr_sample_vertex;
		float batch_sampling_ratio = config.GetBatchSamplingRatio();
		size_t batch_sampling_edges = batch_sampling_ratio * graph.GetNumEdges();
		size_t sample_bitmap_size = (config.GetNumSample() + (sizeof(uint64_t) << 3) - 1) >> 3;
		if (res == NULL) {
			gettimeofday(&start, 0);
			res = new Bitmap(graph.GetMaxVertexId() + 1);

			if(batch_sampling_ratio >= 1.0){
				res->SetAll();
			}else if(batch_sampling_ratio > 0) {
				std::vector<std::pair<VertexId, size_t>> vec;
				vec.reserve(graph.GetMaxVertexId()+1);
				for(int i=0; i<graph.GetMaxVertexId()+1; i++)
					vec.push_back({i, graph.GetInDegree(i)});

				std::sort(vec.begin(), vec.end(),
						[](const std::pair<VertexId, size_t>& a, const std::pair<VertexId, size_t>& b) {
							return a.second < b.second;
						});

				//todo: in-edge vs out-edge
				size_t cnt = 0;
				size_t cnt_sample_vertex = 0;
				size_t cnt_sample_edge = 0;
				for(int i=0; i<vec.size(); i++){
					VertexId v = vec[i].first;
					size_t degree = graph.GetInDegree(v);
					cnt += degree;
					if(cnt > batch_sampling_edges)
						break;

					res->Set(v);
					cnt_sample_vertex++;
					cnt_sample_edge += degree;
				}
				vec.clear();
				BOOST_LOG_TRIVIAL(info) << "cached sample vertex: " << cnt_sample_vertex;
				BOOST_LOG_TRIVIAL(info) << "cached sample edge: " << cnt_sample_edge;
			}

			gettimeofday(&end, 0);
			BOOST_LOG_TRIVIAL(info)  << "vertex selection time: " <<  TimeDiff(start, end);
		}
		return res;
	}

	void BatchSampling(Bitmap* _ptr_sample_vertex = NULL){
		struct timeval start, end;
		gettimeofday(&start, 0);

		size_t sampling_memory = 0;
		if(config.GetBatchSamplingRatio())
			sampling_memory = (graph.GetNumEdges()*sizeof(Bit64*) + graph.GetNumEdges()*config.GetBatchSamplingRatio()*((NUM_SAMPLE+63)/64)*8)/1024/1024;

		BOOST_LOG_TRIVIAL(info)  << "batch sampling memory (mb):" <<  sampling_memory;

		float batch_sampling_ratio = config.GetBatchSamplingRatio();
		BOOST_LOG_TRIVIAL(info)  << "batch sampling ratio: " <<  batch_sampling_ratio;

		if(batch_sampling_ratio > 0) {
			Bitmap *ptr_sample_vertex = SelectVertex(_ptr_sample_vertex);

			SamplingThread **sampling_threads = new SamplingThread *[num_threads];

			sampling_index = new Bit64*[graph.GetNumEdges()];

			VertexAllocator sample_vertex_allocator(ptr_sample_vertex);

			for (int i = 0; i < num_threads; i++)
				sampling_threads[i] = new SamplingThread(i, &sample_vertex_allocator, &graph, sampling_index, config.GetNumSample(), config.GetVertexPartitionSize());

			for (int i = 0; i < num_threads; i++)
				sampling_threads[i]->Start();

			for (int i = 0; i < num_threads; i++)
				sampling_threads[i]->Join();

			for (int i = 0; i < num_threads; i++)
				delete (sampling_threads[i]);

			if (_ptr_sample_vertex == NULL)
				delete ptr_sample_vertex;
		}
		gettimeofday(&end, 0);
		BOOST_LOG_TRIVIAL(info)  << "sampling time: " <<  TimeDiff(start, end);
	}

	template<class QueryThreadType, typename ReturnType>
	ReturnType Query(){
		struct timeval start, end;
		gettimeofday(&start, 0);

		QueryThreadType **query_threads = new QueryThreadType*[num_threads];
		Bitmap query_vertex(graph.GetMaxVertexId() + 1);
		query_vertex.SetAll();

		VertexAllocator query_vertex_allocator(&query_vertex);

		for (int i = 0; i < num_threads; i++)
			query_threads[i] = new QueryThreadType(i, this, &query_vertex_allocator);

		for(int i=0; i<num_threads; i++) {
			query_threads[i]->Start();
		}

		for(int i=0; i<num_threads; i++)
			query_threads[i]->Join();

		for(int i=0; i<num_threads; i++)
			query_threads[0]->Merge(query_threads[i]);

		ReturnType ret = query_threads[0]->GetResult();

		for(int i=0; i<num_threads; i++)
			delete(query_threads[i]);

		gettimeofday(&end, 0);
		BOOST_LOG_TRIVIAL(trace)  << "query time: " <<  TimeDiff(start, end);

		return ret;
	}
};

#endif //SAGE_GRAPH_ENGINE_HPP
