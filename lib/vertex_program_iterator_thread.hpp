#ifndef SAGE_VERTEX_PROGRAM_ITERATOR_HPP
#define SAGE_VERTEX_PROGRAM_ITERATOR_HPP

#include "bitmap.hpp"
#include "graph.hpp"
#include "thread.hpp"
#include "allocator.hpp"
#include "config.hpp"
#include "vertex_program.hpp"
#include "sampling_thread.hpp"
#include "serialized_value_bitmap.hpp"
#include "vertex_cache_aio.hpp"

#define PROGRAM_LOG 0

enum VertexIteratorStep{
	INIT,
    START,
	SCATTER,
	GATHER,
};

template <typename VertexType>
class VertexProgramIteratorThread: public Thread {
	unsigned int tid;
	VertexAllocator* ptr_vertex_allocator;
	unsigned int activated_count;
	Bitmap activated_vertex;
	Bitmap scattered_vertex;
	Bitmap scattered_out_neighbor;
	Bitmap scattered_in_neighbor;
	Bitmap* ptr_merged_scattered_out_neighbor;
	Bitmap* ptr_merged_scattered_in_neighbor;
	Bitmap* ptr_merged_scattered_vertex;
	Graph* ptr_graph;
	VertexProgram<VertexType>* ptr_vertex_program;
	void* ptr_vertex_array;
	VertexIteratorStep vertex_program_iterator_step;
	GraphEngine<VertexType>* ptr_graph_engine;
	size_t vertex_partition_size;
	float batch_sampling_ratio;
	size_t num_sample;
	VertexId current_src;
	VertexId current_dest;
	unsigned int sample_process_mode;
	int fd;
	VertexCache* ptr_vertex_cache;
	VertexCacheAio vertex_cache_aio;
	unsigned int iteration;
	size_t sum_map_size;
	size_t cnt_map_size;
	size_t cnt_program_iter;
	size_t sum_program_iter;

public:
	VertexProgramIteratorThread(unsigned int tid, GraphEngine<VertexType>* ptr_graph_engine, Graph* ptr_graph, int fd, VertexProgram<VertexType>* ptr_vertex_program, void* ptr_vertex_array, VertexAllocator* ptr_vertex_allocator, VertexCache* ptr_vertex_cache)
			: Thread(tid)
			, tid(tid)
			, ptr_graph_engine(ptr_graph_engine)
			, ptr_graph(ptr_graph)
			, fd(fd)
			, ptr_vertex_array(ptr_vertex_array)
			, ptr_vertex_allocator(ptr_vertex_allocator)
			, activated_count(0)
			, scattered_vertex(ptr_graph->GetMaxVertexId()+1)
			, scattered_out_neighbor(ptr_graph->GetMaxVertexId()+1)
			, scattered_in_neighbor(ptr_graph->GetMaxVertexId()+1)
			, ptr_merged_scattered_out_neighbor(NULL)
			, ptr_merged_scattered_in_neighbor(NULL)
			, ptr_merged_scattered_vertex(NULL)
			, activated_vertex(ptr_graph->GetMaxVertexId()+1)
			, current_src(INVALID_VERTEX_ID)
			, current_dest(INVALID_VERTEX_ID)
			, vertex_program_iterator_step(INIT)
			, vertex_cache_aio(ptr_vertex_cache, GetGraphEngine()->GetConfig().GetVertexPartitionSize())
			, ptr_vertex_cache(ptr_vertex_cache) {
		Config& config = GetGraphEngine()->GetConfig();
		this->vertex_partition_size = config.GetVertexPartitionSize();
		this->batch_sampling_ratio = config.GetBatchSamplingRatio();
		this->num_sample = config.GetNumSample();
		this->sample_process_mode = config.GetSampleProcessMode();

		this->sum_map_size = 0;
		this->cnt_map_size = 0;
		this->cnt_program_iter=0;
		this->sum_program_iter=0;
		this->ptr_vertex_program = ptr_vertex_program;
		if(ptr_vertex_program)
			ptr_vertex_program->Init(ptr_graph, this, sample_process_mode);
	}

	~VertexProgramIteratorThread() {
	}

	void Init(){
		sum_map_size = 0;
		cnt_map_size = 0;
		cnt_program_iter = 0;
		sum_program_iter = 0;
		activated_count = 0;
		activated_vertex.ClearAll();
		scattered_vertex.ClearAll();
		scattered_out_neighbor.ClearAll();
		scattered_in_neighbor.ClearAll();
	}

	void* GetVertexPtr(VertexId vid){
		return ptr_graph_engine->GetVertexPtr(vid);
	}

	void AddSumMapSize(size_t add){
		sum_map_size += add;
		cnt_map_size++;
	}

	void AddProgramIter(size_t add){
		cnt_program_iter++;
		sum_program_iter+=add;
	}

	std::pair<size_t, size_t> GetSumMapSize(){
		return {sum_map_size, cnt_map_size};
	}

	std::pair<size_t, size_t> GetProgramIter(){
		return {sum_program_iter, cnt_program_iter};
	}

protected:
	void _RunGatherByDest(VertexId src, VertexId dest, Edge& edge, void** ptr_src_buf, void** ptr_dest_buf, size_t* ptr_buf_size){
		EdgeAttr edge_attr = 0;
		if(GetGraphEngine()->GetConfig().GetHasEdgeAttribute()) {
			EdgeAttr* edge_attrs = ptr_graph->GetAllEdgeAttrs();
			off_t edge_off = &edge - ptr_graph->GetInEdges();
			if(edge_attrs)
				edge_attr = edge_attrs[edge_off];
		}

		Bit64* sampling_data_ptr = NULL;
		bool require_delete = false;
		bool activate_dest;

		if(sample_process_mode == 0 && batch_sampling_ratio == 0){
			assert(0);
//			activate_dest = ptr_vertex_program->_GatherByDest(ptr_src_buf, ptr_dest_buf, edge, NULL, edge_attr, NULL, NULL, NULL);
		}else {
			std::pair<Bit64 *, bool> tmp = GetEdgeSampling(src, dest, edge);
			sampling_data_ptr = tmp.first;
			require_delete = tmp.second;

			Bitmap sampling_bitmap(sampling_data_ptr, num_sample);

			activate_dest = ptr_vertex_program->_GatherByDest(ptr_src_buf, ptr_dest_buf, edge, &sampling_bitmap, edge_attr, ptr_buf_size);
		}

		if(activate_dest){
			activated_count++;
			GetActivatedVertex()->Set(dest);
		}

		if(require_delete)
			delete(sampling_data_ptr);
	}

	void RunGatherByDest() {
		bool certain_graph = GetGraphEngine()->GetConfig().GetCertainGraph();
		int runned_vertex;

//		printf("Gather start\n");
		std::list<std::pair<VertexId, void*>> src_reqs;

		//todo: size 조정
		do {
			runned_vertex = 0;
			std::vector<VertexId> allocated_vertices = ptr_vertex_allocator->GetVertices(1);

			GetGraphEngine()->AddRunningSet(allocated_vertices, false);

			for (int i = 0; i < allocated_vertices.size(); i++) {
				VertexId dest = allocated_vertices[i];

				if (!ptr_merged_scattered_out_neighbor->Get(dest))
					continue;
				assert(ptr_graph->GetInEdgeList(dest).GetDegree() > 0);
				assert(dest < ptr_graph->GetMaxVertexId() + 1);
				void *dest_read_buf = NULL;
				void *dest_write_buf = NULL;
				size_t buf_size;
				vertex_cache_aio.Read(dest, &dest_read_buf, &buf_size);

				posix_memalign(&dest_write_buf, PAGE_SIZE, buf_size);
//				printf("%d -> %p allocated\n", dest, dest_write_buf);
				memcpy(dest_write_buf, dest_read_buf, buf_size);
				vertex_cache_aio.EndRead(dest, dest_read_buf, -1, 0);
				*((VertexId *) dest_write_buf) = dest;

				void* bak = dest_write_buf;
				EdgeList src_list = ptr_graph->GetInEdgeList(dest);

				for (int j = 0; j < src_list.GetDegree(); j++) {
					VertexId src = src_list.GetEdge(j).neighbor;
					void* ptr_edge = &src_list.GetEdge(j);

					if (!ptr_merged_scattered_vertex->Get(src))
						continue;
					src_reqs.push_back({src, ptr_edge});
				}

				while (src_reqs.size() || vertex_cache_aio.GetNumWaitingRequests()) {
					while(src_reqs.size() && vertex_cache_aio.GetNumWaitingRequests() < vertex_partition_size) {
						auto src = src_reqs.front();
						src_reqs.pop_front();
						vertex_cache_aio.ReadAppend(src.first, src.second);
					}
					vertex_cache_aio.SubmitRequests();

					std::vector<VertexRequest> src_events = vertex_cache_aio.WaitForComplete(1);
					for (int j = 0; j < src_events.size(); j++) {
						VertexId src = src_events[j].vid;
						void* src_read_buf = src_events[j].buf;
						assert(src_events[j].rw == READ);
						current_src = src;
						current_dest = dest;

						//todo: Edge&를 어떻게 넘길것인가??
						EdgeList src_list = ptr_graph->GetInEdgeList(dest);
						Edge& edge = *((Edge*) src_events[j].ptr_edge);
						assert (src == edge.neighbor);
#if PROGRAM_LOG
						printf("%d: %d <- %d gather\n", tid, dest, src);
#endif
						_RunGatherByDest(current_src, current_dest, edge, &src_read_buf, &dest_write_buf, &buf_size);

						auto level = GatherSrcSection(src, dest, edge);
						vertex_cache_aio.EndRead(src, src_read_buf, level.first, level.second);
					}
				}
				auto level = GatherDestSection(dest);

				vertex_cache_aio.Write(dest, dest_write_buf, buf_size, level.first, level.second);
				runned_vertex++;
				GetGraphEngine()->RemoveRunningSet(dest);
			}
		} while (runned_vertex > 0);

		current_src = INVALID_VERTEX_ID;
		current_dest = INVALID_VERTEX_ID;
	}

	std::pair<int, double> GatherDestSection(VertexId dest){
#if 1
		size_t num_vertex = ptr_graph->GetMaxVertexId()+1;
		if(ptr_merged_scattered_vertex->Get(dest)){
			EdgeList edge_list = ptr_graph->GetOutEdgeList(dest);
			if(edge_list.GetDegree() > 0) {
				VertexId last = edge_list.GetEdge(edge_list.GetDegree() - 1).neighbor;

				if (dest < last) {
					auto res = GetGraphEngine()->GetCacheSection(false, last);
					return res;
//				return {-1, 0};
				}
			}
		}
#if 1
		bool active = activated_vertex.Get(dest);
		if(active) {
			auto res = GetGraphEngine()->GetCacheSection(true, dest);
			return res;
		}
#endif
#endif
		return {0, 0};
	}

	std::pair<int, double> GatherSrcSection(VertexId src_id, VertexId dest, Edge edge){

		size_t num_vertex = ptr_graph->GetMaxVertexId()+1;

		VertexId next_dest = edge.next_dest;

		if(next_dest != ~0U) {
			bool active = ptr_merged_scattered_out_neighbor->Get(src_id);
			if(active && src_id < next_dest && src_id > dest) {
				auto res = GetGraphEngine()->GetCacheSection(false, src_id);
				return res;
			}else {
				auto res = GetGraphEngine()->GetCacheSection(false, next_dest);
				return res;
			}
		}else {
#if 1
			bool active = ptr_merged_scattered_out_neighbor->Get(src_id);
			if(active && dest < src_id) {
				auto res = GetGraphEngine()->GetCacheSection(true, src_id);
				return res;
			}
#endif
		}
		return {0, 0};
	}

	std::pair<int, double> ScatterSection(VertexId src_id){
#if 0
		size_t num_vertex = ptr_graph->GetMaxVertexId()+1;
		bool scatter = scattered_vertex.Get(src_id);
		if(scatter) {
			EdgeList edge_list = ptr_graph->GetOutEdgeList(src_id);
			size_t degree = edge_list.GetDegree();

			VertexId first = 0;
			if(degree) {
				first = edge_list.GetEdge(0).neighbor;
				auto res = GetGraphEngine()->GetCacheSection(false, first);
				return res;
			}
		}
#endif
		return {0, 0};
	}

	void RunScatterBySrc(){
		int runned_vertex;
		std::list<VertexId> src_reqs;

//		printf("scatter start\n");
		do {
			runned_vertex = 0;
			std::vector<VertexId> allocated_vertices = ptr_vertex_allocator->GetVertices(vertex_partition_size);

			GetGraphEngine()->AddRunningSet(allocated_vertices, true);
			for(int i=0; i< allocated_vertices.size(); i++) {
				VertexId src = allocated_vertices[i];
				vertex_cache_aio.ReadAppend(src, NULL);
			}
			vertex_cache_aio.SubmitRequests();

			while(vertex_cache_aio.GetNumWaitingRequests()) {
				std::vector<VertexRequest> src_events = vertex_cache_aio.WaitForComplete(1);

				for (int j = 0; j < src_events.size(); j++) {
					VertexId src = src_events[j].vid;
					void* read_buf = src_events[j].buf;
					size_t buf_size = src_events[j].size;

					void* write_buf = NULL;
					posix_memalign(&write_buf, PAGE_SIZE, buf_size);
					memcpy(write_buf, read_buf, buf_size);
					auto level = ScatterSection(src);
					vertex_cache_aio.EndRead(src, read_buf, level.first, level.second);
					*((VertexId*) write_buf) = src;

					EdgeList out_edge_list = ptr_graph->GetOutEdgeList(src);
					EdgeList in_edge_list = ptr_graph->GetInEdgeList(src);
					current_src = src;
#if PROGRAM_LOG
					printf("%d: %d scatter\n", tid, src);
#endif

					ptr_vertex_program->_ScatterBySrc(&write_buf,
						                                  out_edge_list, in_edge_list, &buf_size);
					runned_vertex++;
					vertex_cache_aio.Write(src, write_buf, buf_size, level.first, level.second);
					GetGraphEngine()->RemoveRunningSet(src);
				}
			}
		} while (runned_vertex > 0);
		current_src = INVALID_VERTEX_ID;
		current_dest = INVALID_VERTEX_ID;
	}

    void RunStart(){
        int runned_vertex;
        std::list<VertexId> src_reqs;
        do {
            runned_vertex = 0;
            std::vector<VertexId> allocated_vertices = ptr_vertex_allocator->GetVertices(vertex_partition_size);

            GetGraphEngine()->AddRunningSet(allocated_vertices, true);
            for(int i=0; i< allocated_vertices.size(); i++) {
                VertexId src = allocated_vertices[i];
                vertex_cache_aio.ReadAppend(src, NULL);
            }
//            std::cout << allocated_vertices.size() << " vertex set starts\n";
            vertex_cache_aio.SubmitRequests();

            while(vertex_cache_aio.GetNumWaitingRequests()) {
                std::vector<VertexRequest> src_events = vertex_cache_aio.WaitForComplete(1);

                for (int j = 0; j < src_events.size(); j++) {
                    VertexId src = src_events[j].vid;
                    void* read_buf = src_events[j].buf;
                    size_t buf_size = src_events[j].size;

                    void* write_buf = NULL;
                    posix_memalign(&write_buf, PAGE_SIZE, buf_size);
                    memcpy(write_buf, read_buf, buf_size);
                    vertex_cache_aio.EndRead(src, read_buf, 0, 0);
                    *((VertexId*) write_buf) = src;

                    EdgeList out_edge_list = ptr_graph->GetOutEdgeList(src);
                    EdgeList in_edge_list = ptr_graph->GetInEdgeList(src);
                    current_src = src;
#if PROGRAM_LOG
                    printf("%d: %d init value\n", tid, src);
#endif
                    if(sample_process_mode == 1) {
                        SerializedBundleVertex<VertexType> src_vertex_attr(src, num_sample, write_buf);
                        if(ptr_graph_engine->call_start_func) {
                            for (int i = 0; i < num_sample; i++) {
                                VertexType *vertex = src_vertex_attr.GetVertexAttributeByIndex(i);
                                vertex->Start();
                            }
                        }
                        Bitmap activate_bitmap = src_vertex_attr.GetActivationBitmap(1);
                        activate_bitmap.SetAll();
                    }else if(sample_process_mode == 2) {
                        SerializedSimVertex<VertexType> src_vertex_attr(src, num_sample, &write_buf);
                        VertexType start_v;

                        if(ptr_graph_engine->call_start_func) {
                            std::pair<VertexType *, Bitmap> entry = src_vertex_attr.GetValueBitmapByIndex(0);
                            start_v.Start();
                            *(entry.first) = start_v;
                            entry.second.SetAll();
                        }
                        Bitmap activate_bitmap = src_vertex_attr.GetActivationBitmap(1);
                        activate_bitmap.SetAll();
                    }else if(sample_process_mode == 3) {
                        SerializedSimVertex<VertexType> src_vertex_attr(src, num_sample, &write_buf);
                        VertexType start_v;

                        if(ptr_graph_engine->call_start_func) {
                            std::pair<VertexType *, Bitmap> entry = src_vertex_attr.GetValueBitmapByIndex(0);
                            start_v.Start();
                            *(entry.first) = start_v;
                            entry.second.SetAll();
                        }
                        Bitmap activate_bitmap = src_vertex_attr.GetActivationBitmap(1);
                        activate_bitmap.SetAll();
                    }

                    runned_vertex++;
                    vertex_cache_aio.Write(src, write_buf, buf_size, 0, 0);
                    GetGraphEngine()->RemoveRunningSet(src);
                }
            }
        } while (runned_vertex > 0);
        current_src = INVALID_VERTEX_ID;
        current_dest = INVALID_VERTEX_ID;
    }

	void RunInit(){
		if(!INIT_ALL_VALUE)
			return;

		int runned_vertex;
		std::list<VertexId> src_reqs;
		do {
			runned_vertex = 0;
			std::vector<VertexId> allocated_vertices = ptr_vertex_allocator->GetVertices(vertex_partition_size);

			GetGraphEngine()->AddRunningSet(allocated_vertices, true);
			for(int i=0; i< allocated_vertices.size(); i++) {
				VertexId src = allocated_vertices[i];
				vertex_cache_aio.ReadAppend(src, NULL);
			}
			vertex_cache_aio.SubmitRequests();

			while(vertex_cache_aio.GetNumWaitingRequests()) {
				std::vector<VertexRequest> src_events = vertex_cache_aio.WaitForComplete(1);

				for (int j = 0; j < src_events.size(); j++) {
					VertexId src = src_events[j].vid;
					void* read_buf = src_events[j].buf;
					size_t buf_size = src_events[j].size;

					void* write_buf = NULL;
					posix_memalign(&write_buf, PAGE_SIZE, buf_size);
					memcpy(write_buf, read_buf, buf_size);
					vertex_cache_aio.EndRead(src, read_buf, 0, 0);
					*((VertexId*) write_buf) = src;

					EdgeList out_edge_list = ptr_graph->GetOutEdgeList(src);
					EdgeList in_edge_list = ptr_graph->GetInEdgeList(src);
					current_src = src;
#if PROGRAM_LOG
					printf("%d: %d init value\n", tid, src);
#endif
					if(sample_process_mode == 1) {
						SerializedBundleVertex<VertexType> src_vertex_attr(src, num_sample, write_buf);
						for (int i = 0; i < num_sample; i++) {
							VertexType *vertex = src_vertex_attr.GetVertexAttributeByIndex(i);
							vertex->Init();
						}
					}else if(sample_process_mode == 2) {
						SerializedSimVertex<VertexType> src_vertex_attr(src, num_sample, &write_buf);
						assert(src_vertex_attr.GetNumValue() == 1);
						auto it = src_vertex_attr.GetValueBitmapByIndex(0);
//						assert(it.second.IsAllSet());
						it.first->Init();
                    }else if(sample_process_mode == 3) {
                        SerializedSimVertex<VertexType> src_vertex_attr(src, num_sample, &write_buf);
                        assert(src_vertex_attr.GetNumValue() == 1);
                        auto it = src_vertex_attr.GetValueBitmapByIndex(0);
//                        assert(it.second.IsAllSet());
                        it.first->Init();
					}

					runned_vertex++;
					vertex_cache_aio.Write(src, write_buf, buf_size, 0, 0);
					GetGraphEngine()->RemoveRunningSet(src);
				}
			}
		} while (runned_vertex > 0);
		current_src = INVALID_VERTEX_ID;
		current_dest = INVALID_VERTEX_ID;
	}

	void Run() {
		iteration = GetGraphEngine()->GetCurrentIteration();

		switch(vertex_program_iterator_step) {
            std::cout << "aaaa\n";
            case INIT:
				RunInit();
				break;
            case START:
                RunStart();
                break;
			case SCATTER:
				RunScatterBySrc();
				break;
			case GATHER:
				RunGatherByDest();
				break;
			default:
				assert(0);
		}
	}

public:
	std::pair<Bit64* , bool> GetEdgeSampling(VertexId src, VertexId dest, Edge& edge, bool in_edge = true){
		Bit64* sampling_data_ptr = NULL;
		bool require_delete = false;
		if(GetGraphEngine()->GetConfig().GetCertainGraph()) {
			return {GetGraphEngine()->GetAllTrueBitmap().GetData(), false};
		}

		if(batch_sampling_ratio > 0){
			off_t off;
			if(in_edge)
				off = (&edge) - ptr_graph->GetInEdges();
			else
				off = (&edge) - ptr_graph->GetOutEdges();
			Bit64** sampling_index = GetGraphEngine()->GetSamplingIndex();
			sampling_data_ptr = sampling_index[off];
			if(sampling_data_ptr == NULL){
				sampling_data_ptr = BaseBitmap::Allocate(num_sample);
				BaseBitmap::ClearAll(sampling_data_ptr, num_sample);
				SamplingThread::Sampling(sampling_data_ptr, src, dest, num_sample, edge.probability);
				require_delete = true;
			}
		}else{
			sampling_data_ptr = BaseBitmap::Allocate(num_sample);
			BaseBitmap::ClearAll(sampling_data_ptr, num_sample);
			SamplingThread::Sampling(sampling_data_ptr, src, dest, num_sample, edge.probability);
			require_delete = true;
		}
		return {sampling_data_ptr, require_delete};
	}

	inline GraphEngine<VertexType>* GetGraphEngine(){
		return ptr_graph_engine;
	}

	inline void SetStep(VertexIteratorStep step){
		vertex_program_iterator_step = step;
	}

	inline void SetVertexAllocator(VertexAllocator* ptr_vertex_allocator){
		this->ptr_vertex_allocator = ptr_vertex_allocator;
	}

	inline void SetVertexProgram(VertexProgram<VertexType>* ptr_vertex_program){
		this->ptr_vertex_program = ptr_vertex_program;
		if(ptr_vertex_program)
			ptr_vertex_program->Init(ptr_graph, this, sample_process_mode);
	}

	inline unsigned int GetActivatedCount(){
		return activated_count;
	}

	inline Bitmap* GetActivatedVertex(){
		return &activated_vertex;
	}

	inline Bitmap* GetScatteredVertex(){
		return &scattered_vertex;
	}

	inline Bitmap* GetScatteredInNeighbor(){
		return &scattered_in_neighbor;
	}

	inline Bitmap* GetScatteredOutNeighbor(){
		return &scattered_out_neighbor;
	}

	inline VertexId GetCurrentSrcId(){
		return current_src;
	}

	inline VertexId GetCurrentDestId(){
		return current_dest;
	}

	inline void SetMergedScatteredOutNeighbor(Bitmap* ptr_merged_scattered_out_neighbor){
		this->ptr_merged_scattered_out_neighbor = ptr_merged_scattered_out_neighbor;
	}

	inline void SetMergedScatteredInNeighbor(Bitmap* ptr_merged_scattered_in_neighbor){
		this->ptr_merged_scattered_in_neighbor = ptr_merged_scattered_in_neighbor;
	}

	inline void SetMergedScatteredVertex(Bitmap* ptr_merged_scattered_vertex){
		this->ptr_merged_scattered_vertex = ptr_merged_scattered_vertex;
	}
};

#endif //SAGE_VERTEX_PROGRAM_ITERATOR_HPP
