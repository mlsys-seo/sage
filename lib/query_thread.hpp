#ifndef SAGE_QUERYTHREAD_HPP
#define SAGE_QUERYTHREAD_HPP
#include "allocator.hpp"
#include "config.hpp"
#include "vertex_cache_aio.hpp"

template<typename VertexType, typename ReturnType>
class QueryThread: public Thread {
	unsigned int tid;
	unsigned int vertex_partition_size;
	VertexAllocator* ptr_vertex_allocator;
	void* ptr_vertex_array;
	GraphEngine<VertexType>* ptr_graph_engine;
	Graph* ptr_graph;
	VertexId current_vertex_id;
	unsigned int sample_process_mode;
	Config config;
	int step;
	VertexCacheAio vertex_cache_aio;
	VertexCache* ptr_vertex_cache;
    size_t cnt_type0=0,cnt_type1=0, cnt_value = 0;

public:
	QueryThread(){

	}

	QueryThread(unsigned int tid, GraphEngine<VertexType>* ptr_graph_engine, VertexAllocator* ptr_vertex_allocator)
			: Thread(tid)
			, tid(tid)
			, current_vertex_id(INVALID_VERTEX_ID)
			, ptr_vertex_allocator(ptr_vertex_allocator)
			, ptr_graph_engine(ptr_graph_engine)
			, config(ptr_graph_engine->GetConfig())
			, ptr_vertex_cache(ptr_graph_engine->GetVertexCache())
			, vertex_cache_aio(ptr_graph_engine->GetVertexCache(), ptr_graph_engine->GetConfig().GetVertexPartitionSize()){
		ptr_vertex_array = NULL;
		ptr_graph = ptr_graph_engine->GetGraph();
		vertex_partition_size = config.GetVertexPartitionSize();
		sample_process_mode = config.GetSampleProcessMode();
		step = 0;
	}

	void SetVertexAllocator(VertexAllocator* ptr_vertex_allocator){
		this->ptr_vertex_allocator = ptr_vertex_allocator;
	}

	inline Graph* GetPtrGraph(){
		return ptr_graph;
	}
	inline VertexId GetCurrentVertexId(){
		return current_vertex_id;
	}

	virtual ReturnType GetResult() = 0;

	virtual void Print(){
	}

	virtual void Merge(QueryThread* query_thread) = 0;

	virtual void Query(VertexType& vertex, size_t duplicated = 1) = 0;

	virtual void GlobalQuery(VertexId vertex_id) {
	}

	void Run() {
		int runned_vertex;
		std::list<VertexId> src_reqs;

        do {
			runned_vertex = 0;
			std::vector<VertexId> allocated_vertices = ptr_vertex_allocator->GetVertices(vertex_partition_size);

			ptr_graph_engine->AddRunningSet(allocated_vertices, true);
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
					current_vertex_id = src;
#if PROGRAM_LOG
					printf("%d: %d query\n", tid, src);
#endif
					if (sample_process_mode == 1) {
						SerializedBundleVertex<VertexType> src_vertex_attr(src, NUM_SAMPLE, write_buf);

						for (int i = 0; i < NUM_SAMPLE; i++) {
							VertexType *ptr_vertex = src_vertex_attr.GetVertexAttributeByIndex(i);
							Query(*ptr_vertex, 1);
						}
					} else if (sample_process_mode == 2) {
						SerializedSimVertex<VertexType> src_vertex_attr(src, NUM_SAMPLE, &write_buf);

						for (int i = 0; i < src_vertex_attr.GetNumValue(); i++) {
							auto entry = src_vertex_attr.GetValueBitmapByIndex(i);
							Query(*entry.first, entry.second.GetSetCount());
						}
						cnt_value += src_vertex_attr.GetNumValue();
                    } else if (sample_process_mode == 3) {
					    int vertex_type = *(int*)((uint8_t*) write_buf + sizeof(VertexId));
					    if(vertex_type == 0){
                            SerializedBundleVertex<VertexType> src_vertex_attr(src, NUM_SAMPLE, write_buf);

                            for (int i = 0; i < NUM_SAMPLE; i++) {
                                VertexType *ptr_vertex = src_vertex_attr.GetVertexAttributeByIndex(i);
                                Query(*ptr_vertex, 1);
                            }
                            cnt_type0++;
					    }else if(vertex_type == 1){
                            SerializedSimVertex<VertexType> src_vertex_attr(src, NUM_SAMPLE, &write_buf);

                            for (int i = 0; i < src_vertex_attr.GetNumValue(); i++) {
                                auto entry = src_vertex_attr.GetValueBitmapByIndex(i);
                                Query(*entry.first, entry.second.GetSetCount());
                            }
                            cnt_type1++;
                            cnt_value += src_vertex_attr.GetNumValue();
					    }

					}

					runned_vertex++;
					vertex_cache_aio.Write(src, write_buf, buf_size, 0, 0);

					GlobalQuery(current_vertex_id);
					ptr_graph_engine->RemoveRunningSet(src);
				}
			}
		} while (runned_vertex > 0);
        ptr_graph_engine->SumStat(1, cnt_type0, cnt_type1, cnt_value, 0, 0);
    }
};

#endif //SAGE_QUERYTHREAD_HPP
