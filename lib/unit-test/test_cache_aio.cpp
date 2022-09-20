#ifndef SAGE_CACHED_AIO_HPP
#define SAGE_CACHED_AIO_HPP

#include "thread.hpp"
#include "graph.hpp"
#include "cache_aio.hpp"
#include <queue>
#include <atomic>
#include "allocator.hpp"
#include "bitmap.hpp"
#include "serialized_value_bitmap.hpp"

const unsigned int num_worker = 16;
const unsigned int part_size = 32;

size_t num_sample = 100000;

DiskOffsetAllocator disk_offset_allocator;

struct V{
	int a;
	int b;
};

struct VertexMap{
	off_t offset;
	int size;
	unsigned int program_cnt;
};

struct VertexMap* vertex_map;
void* ptr_init_value;

class Worker: public Thread {
	unsigned int wid;
	unsigned int part_size;
	VertexAllocator* ptr_vertex_allocator;
	int fd_vertex;
	Graph* ptr_graph;
	Aio dest_read_aio;
	Aio dest_write_aio;
	Aio src_aio;
	Cache* ptr_cache;
	CacheAio src_cache_aio;
	CacheAio dest_read_cache_aio;
	CacheAio dest_write_cache_aio;
//	float res;
	int step;

public:
	Worker(unsigned int wid, unsigned int part_size, VertexAllocator* ptr_vertex_allocator, Graph* ptr_graph, int fd_vertex, Cache* ptr_cache)
			: Thread(wid)
			, wid(wid)
			, part_size(part_size)
			, ptr_vertex_allocator(ptr_vertex_allocator)
			, ptr_graph(ptr_graph)
			, dest_read_aio(part_size)
			, dest_write_aio(part_size)
			, src_aio(part_size)
			, src_cache_aio(&src_aio, ptr_cache)
			, dest_read_cache_aio(&dest_read_aio, ptr_cache, true)
			, dest_write_cache_aio(&dest_write_aio, ptr_cache, true)
			, ptr_cache(ptr_cache)
			, fd_vertex(fd_vertex){
		step = 0;
//		std::cout << wid << " worker init\n";
	}

	void SetVertexAllocator(VertexAllocator* ptr_vertex_allocator){
		this->ptr_vertex_allocator = ptr_vertex_allocator;
	}

	void SetStep(int s){
		step = s;
	}

	void Run(){
		if(step == 0)
			Scatter();
		else
			Gather();
	};

	void Scatter(){
		std::vector<VertexId> allocated_vertex;
		allocated_vertex = ptr_vertex_allocator->GetVertices(part_size);
		if(allocated_vertex.size() == 0)
			return;

		int src_alloc=0;
		do {
			for(int i=0; i<allocated_vertex.size(); i++){
				VertexId src = allocated_vertex[i];
				//todo: active(src)
				src_alloc++;
				src_cache_aio.AsyncRequest(MakeVertexRequest(src, READ));
			}

			while(src_cache_aio.GetQueueSize() || src_cache_aio.GetNumWaitingRequests()) {
				src_cache_aio.SubmitRequests();
				std::vector<IoRequest> src_events = src_cache_aio.WaitForComplete(1);

				for (int i = 0; i < src_events.size(); i++) {
					if(src_events[i].rw == READ || src_events[i].rw == READ_INIT_VALUE) {
						long long offset;
						unsigned long size;
						uint8_t *src_buf;
						VertexId src;
						if(src_events[i].rw == READ){
							offset = src_events[i].offset;
							size = src_events[i].size;
							src_buf = (uint8_t *) src_events[i].buf;
							src = *((VertexId *) src_buf);

							assert(offset == vertex_map[src].offset);
							assert(size == vertex_map[src].size);

#if 1
							SerializedBundleVertex<V> vertex_array(src, num_sample, src_buf);

							for(int i=0; i<num_sample; i++) {
								V* v = vertex_array.GetVertexAttributeByIndex(i);
								assert(v->a == i);
								assert(v->b == src);
							}
#endif
						}else if (src_events[i].rw == READ_INIT_VALUE){
//							offset = disk_offset_allocator.Alloc(PAGE_SIZE);
//							size = PAGE_SIZE;
							src_buf = (uint8_t *) src_events[i].buf;
							src = *((VertexId *) src_buf);

#if 1
							SerializedBundleVertex<V> vertex_array(src, num_sample, src_buf);
							for(int i=0; i<num_sample; i++) {
								V* v = vertex_array.GetVertexAttributeByIndex(i);
								v->a = i;
								v->b = src;
							}
#endif
						}


//						printf("scatter : %d\n", src);

//todo: check src_update
//todo: not update -> free
						src_cache_aio.AsyncRequest(MakeVertexRequest(src, WRITE, src_buf));

					}else if (src_events[i].rw == WRITE){
//						printf("scatter update : %d\n", *((VertexId*) src_events[i].buf));
						free(src_events[i].buf);
						src_alloc --;
					}
				}
			}

			assert(src_alloc == 0);
			allocated_vertex = ptr_vertex_allocator->GetVertices(part_size);
		}while(allocated_vertex.size());
	}

	void Gather(){
		std::vector<VertexId> allocated_vertex;
		allocated_vertex = ptr_vertex_allocator->GetVertices(part_size);

		if(allocated_vertex.size() == 0)
			return;

		int src_alloc=0, dest_alloc=0;
		do {
			for(int i=0; i<allocated_vertex.size(); i++){
				VertexId dest = allocated_vertex[i];
				//todo: active(dest)
				if(ptr_graph->GetInEdgeList(dest).GetDegree()) {
					dest_alloc++;
					dest_read_cache_aio.AsyncRequest(MakeVertexRequest(dest, READ));
				}
			}

			while(dest_read_cache_aio.GetQueueSize() || dest_read_cache_aio.GetNumWaitingRequests()) {
				dest_read_cache_aio.SubmitRequests();
				std::vector<IoRequest> dest_events = dest_read_cache_aio.WaitForComplete(1);

				for (int i = 0; i < dest_events.size(); i++) {
					long long offset;
					unsigned long size;
					uint8_t* dest_buf;
					VertexId dest;
					if(dest_events[i].rw == READ) {
						offset = dest_events[i].offset;
						size = dest_events[i].size;
						dest_buf = (uint8_t *) dest_events[i].buf;
						dest = *((VertexId *) dest_buf);

						//todo:
					}else if(dest_events[i].rw == READ_INIT_VALUE){
//						offset = disk_offset_allocator.Alloc(PAGE_SIZE);
//						size = PAGE_SIZE;
						dest_buf = (uint8_t *) dest_events[i].buf;
						dest = *((VertexId *) dest_buf);
#if 1
						SerializedBundleVertex<V> vertex_array(dest, num_sample, dest_buf);
						for(int i=0; i<num_sample; i++) {
							V* v = vertex_array.GetVertexAttributeByIndex(i);
							v->a = i;
							v->b = dest;
						}
#endif
					} else
						assert(0);

					EdgeList src_list = ptr_graph->GetInEdgeList(dest);
					for (int j = 0; j < src_list.GetDegree(); j++) {
						VertexId src = src_list.GetEdge(j).neighbor;
						//todo : active(src)
						src_alloc++;
						src_cache_aio.AsyncRequest(MakeVertexRequest(src, READ, NULL, (void *) dest_buf));
						vertex_map[dest].program_cnt++;
					}
				}
			}

			while (src_cache_aio.GetQueueSize() || src_cache_aio.GetNumWaitingRequests()) {
				src_cache_aio.SubmitRequests();
				std::vector<IoRequest> src_events = src_cache_aio.WaitForComplete(1);
				for (int i = 0; i < src_events.size(); i++) {
					long long offset;
					unsigned long size;
					uint8_t * dest_buf;
					uint8_t * src_buf;
					VertexId dest;
					VertexId src;

					if(src_events[i].rw == READ) {
						offset = src_events[i].offset;
						size = src_events[i].size;
						src_buf = src_events[i].buf;
						dest_buf = (uint8_t *) src_events[i].callback;
						src = *((VertexId *) src_buf);
						dest = *((VertexId *) dest_buf);


#if 1
						SerializedBundleVertex<V> dest_vertex_array(dest, num_sample, dest_buf);
						SerializedBundleVertex<V> src_vertex_array(src, num_sample, src_buf);

						/*
						for(int i=0; i<num_sample; i++) {
							V* v = dest_vertex_array.GetVertexByIndex(i);
							assert(v->a == i);
							assert(v->b == dest);
						}

						for(int i=0; i<num_sample; i++) {
							V* v = src_vertex_array.GetVertexByIndex(i);
							assert(v->a == i);
							assert(v->b == dest);
						}
						 */
#endif


					}else if(src_events[i].rw == READ_INIT_VALUE){
						src_buf = (uint8_t *) src_events[i].buf;
						dest_buf = (uint8_t *) src_events[i].callback;

						src = *((VertexId *) src_buf);
						dest = *((VertexId *) dest_buf);

#if 1
						SerializedBundleVertex<V> src_vertex_array(src, num_sample, src_buf);
						for(int i=0; i<num_sample; i++) {
							V* v = src_vertex_array.GetVertexAttributeByIndex(i);
							v->a = i;
							v->b = src;
						}
#endif
					} else
						assert(0);

//					printf("%d->%d\n", src, dest);

					vertex_map[dest].program_cnt--;
					free(src_buf);
					src_alloc--;
					if(vertex_map[dest].program_cnt==0){
						int ret = dest_write_cache_aio.AsyncRequest(MakeVertexRequest(dest, WRITE, dest_buf));
						dest_write_cache_aio.SubmitRequests();
					}
				}
			}

			while(dest_write_cache_aio.GetQueueSize() || dest_write_cache_aio.GetNumWaitingRequests()) {
				dest_write_cache_aio.SubmitRequests();
				std::vector<IoRequest> dest_events = dest_write_cache_aio.WaitForComplete(1);
				for (int i = 0; i < dest_events.size(); i++) {
					uint8_t *dest_buf = (uint8_t *) dest_events[i].buf;
					VertexId dest = *((VertexId*) dest_buf);
					free(dest_buf);
					dest_alloc--;
				}
			}
			assert(src_alloc == 0);
			assert(dest_alloc == 0);

			allocated_vertex = ptr_vertex_allocator->GetVertices(part_size);
		}while(allocated_vertex.size());

//		std::cout << wid << " worker end\n";
	}

	inline IoRequest MakeVertexRequest(VertexId vertex, uint8_t rw = READ, uint8_t* buf = NULL, void* callback = NULL){
		IoRequest ret;
		ret.size = vertex_map[vertex].size;
		ret.offset = vertex_map[vertex].offset;
		ret.rw = rw;
		ret.fd = fd_vertex;
		ret.callback = (void*) callback;

		// callback != NULL :: read_only (src vertex at Gather)...
		if(rw == READ) {
			assert(buf == NULL);
			posix_memalign((void **) &ret.buf, PAGE_SIZE, ret.size);
		}else if(rw == WRITE){
			assert(buf != NULL);
			assert(*((VertexId*)buf) == vertex);
			ret.buf = buf;
		}

		if(ret.offset == ~0UL) {
			if (rw == READ) {
				ret.rw = READ_INIT_VALUE;
				memcpy(ret.buf, ptr_init_value, ret.size);
				*((VertexId *) ret.buf) = vertex;
			} else if (rw == WRITE) {
				ret.offset = disk_offset_allocator.Alloc(ret.size);
				vertex_map[vertex].offset = ret.offset;
				vertex_map[vertex].size = ret.size;
				ret.buf = buf;
			}
		}

		return ret;
	}
};

int main(int argc, char** argv) {
	Graph g;
	g.Load(argv[1]);

	std::string filename = "/home/kckjn97/sage.tmp";
	std::cout << filename << "\n";
	int fd = open(filename.c_str(), O_RDWR| O_CREAT | O_DIRECT, 0644);
	if (fd < 0) {
		fprintf(stderr, "not exist %d\n", fd);
		exit(-1);
	}

	//vertex_map = new VertexMap[g.GetMaxVertexId()+1];
	vertex_map = (struct VertexMap*) malloc(sizeof(struct VertexMap)*(g.GetMaxVertexId()+1));
	int alloc_unit_size = SerializedBundleVertex<V>::GetAllocPageUnit(num_sample) * PAGE_SIZE;
	for(int i=0; i<g.GetMaxVertexId()+1; i++){
		vertex_map[i].offset = ~0UL;
		vertex_map[i].size = alloc_unit_size;
		vertex_map[i].program_cnt = 0;
	}

	posix_memalign((void **) &ptr_init_value, PAGE_SIZE, alloc_unit_size);
	memset(ptr_init_value, 0x00, alloc_unit_size);
	SerializedBundleVertex<V> init_vertex(0, num_sample, ptr_init_value);
	init_vertex.GetActivationBitmap(0).SetAll();
	init_vertex.GetActivationBitmap(1).SetAll();

	//Cache Init
	Cache cache(fd, 1024);

	//preload
#if 0
	Aio *aio = new Aio(128);
	uint8_t *buf;
	posix_memalign((void **) &buf, PAGE_SIZE, PAGE_SIZE);
	memset(buf, 0xff, PAGE_SIZE);

	CacheAio cache_aio(aio, &cache);
	for (int i = 0; i <(g.GetMaxVertexId()+1)/1; i++) {
		off_t off = i * PAGE_SIZE;
		cache_aio.Read({fd, off, PAGE_SIZE, buf, READ});
		//aio->SyncRequest({fd, off, PAGE_SIZE, buf, READ});
		int *p_buf = (int *) buf;
		assert(i == p_buf[0]);
	}
	cache.StatInit();
	//Cache** cache = new Cache*[num_worker];
#endif

	struct timeval start, end;

	for(int k=0; k<1;k++) {
		Bitmap activated_vertex(g.GetMaxVertexId()+1);
		activated_vertex.SetAll();
		VertexAllocator vertex_allocator(&activated_vertex);

		gettimeofday(&start, NULL);
		std::vector<Worker*> workers;
		for (int i = 0; i < num_worker; i++) {
			workers.push_back(new Worker(i, part_size, &vertex_allocator, &g, fd, &cache));
		}

#if 0
		std::cout << "Scatter\n";
		for (int i = 0; i < num_worker; i++) {
			workers[i]->SetStep(0);
			workers[i]->Start();
		}

		for (int i = 0; i < num_worker; i++)
			workers[i]->Join();
		gettimeofday(&end, NULL);
		std::cout << "elapsed_time: " << TimeDiff(start, end) << "\n\n";
#endif

#if 1
		VertexAllocator vertex_allocator2(&activated_vertex);
		gettimeofday(&start, NULL);
		std::cout << "Gather\n";
		for (int i = 0; i < num_worker; i++) {
			workers[i]->SetStep(1);
			workers[i]->SetVertexAllocator(&vertex_allocator2);
			workers[i]->Start();
		}

		for (int i = 0; i < num_worker; i++)
			workers[i]->Join();
#endif
		gettimeofday(&end, NULL);
		std::cout << "elapsed_time: " << TimeDiff(start, end) << "\n\n";
	}
	close(fd);
};
#endif //SAGE_CACHED_AIO_HPP

