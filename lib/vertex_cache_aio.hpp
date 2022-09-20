#ifndef SAGE_VERTEX_CACHE_AIO_HPP
#define SAGE_VERTEX_CACHE_AIO_HPP

#include <cassert>
#include "aio.hpp"
#include "vertex_cache.hpp"


struct VertexRequest{
	VertexId vid;
	void* buf;
	uint8_t rw;
	size_t size;
	void* ptr_edge;
};

class VertexCacheAio {
	Aio aio;
	VertexCache* ptr_vertex_cache;
	std::queue<VertexRequest> cached_queue;

public:
	VertexCacheAio(VertexCache* ptr_vertex_cache, size_t queue_depth) :
			aio(queue_depth),
			ptr_vertex_cache(ptr_vertex_cache){
	}

	~VertexCacheAio() {

	}

	inline size_t GetCachedQueueSize(){
		return cached_queue.size();
	}

	inline size_t GetQueueSize(){
		return aio.GetQueueSize()+cached_queue.size();
	}

	inline size_t GetNumWaitingRequests() {
		size_t res = aio.GetQueueSize() + aio.GetNumWaitingRequests()+cached_queue.size();
		return res;
	}

	inline size_t RemainingQueueDepth() {
		return aio.RemainingQueueDepth();
	}

	//write는 write-back이라서 async나 sync 똑같다...
	inline int ReadAppend(VertexId vertex, void* ptr_edge){
		VertexRequest vertex_req;
		IoRequest io_req;
		void* buf = NULL;
		size_t size;
		int ret = ptr_vertex_cache->Get(vertex, &buf, &size, &io_req);
		if (ret == CACHE_HIT) {
			vertex_req.vid = vertex;
			vertex_req.rw = READ;
			vertex_req.buf = buf;
			vertex_req.size = size;
			vertex_req.ptr_edge = ptr_edge;
			cached_queue.push(vertex_req);
			return CACHE_HIT;
		}
		io_req.callback = ptr_edge;
		aio.AsyncRequest(io_req);
		return CACHE_MISS;
	}

	inline void EndRead(VertexId vertex, void* buf, int section = 0, double priority = 0){
		ptr_vertex_cache->EndRead(vertex, buf, section, priority);
	}

	inline void Read(VertexId vertex, void** ptr_buf, size_t* ptr_size){
		//todo: multi page
		IoRequest io_req;
		int ret = ptr_vertex_cache->Get(vertex, ptr_buf, ptr_size, &io_req);
		if (ret == CACHE_HIT)
			return;

		aio.SyncRequest(io_req);
		ret = ptr_vertex_cache->Put(vertex, (void **) &io_req.buf, &io_req.size, 1, 0, true);
		*ptr_buf = io_req.buf;
		*ptr_size = io_req.size;
		return;
	}

	inline void Write(VertexId vertex, void* buf, size_t size, int section = -1, double priority = 0){
		//todo: multi page
    	assert(vertex == *((VertexId*) buf));
		ptr_vertex_cache->Put(vertex, (void**) &buf, &size, section, priority);
	}

	size_t SubmitRequests() {
		return aio.SubmitRequests();
	};

	std::vector<VertexRequest> WaitForComplete(size_t min_requests, size_t max_requests = 256) {
		int num_completed = 0;
		std::vector<VertexRequest> res;
		res.reserve(max_requests);

		for(int i=0; i<cached_queue.size(); i++) {
			VertexRequest cached_vertex_req = cached_queue.front();
			cached_queue.pop();
			res.push_back(cached_vertex_req);
			num_completed++;

			if(num_completed>= max_requests)
				break;
		}

		if(aio.GetNumWaitingRequests() && num_completed < max_requests){
			std::vector < io_event> events = aio.WaitForComplete(1);
			//callback
			for (int i = 0; i < events.size(); i++) {
				iocb *p_iocb = events[i].obj;

				VertexRequest vertex_req;
				assert(p_iocb->aio_lio_opcode == IO_CMD_PREAD);
				VertexId vertex = *((VertexId*) p_iocb->u.c.buf);
#if IO_LOG
				printf("%d (%p) read miss write\n", *((VertexId*) vertex_req.buf), vertex_req.buf);
#endif
				int ret = ptr_vertex_cache->Put(vertex, (void**) &p_iocb->u.c.buf, &p_iocb->u.c.nbytes, 1, 0, true);

				vertex_req.buf = p_iocb->u.c.buf;
				vertex_req.vid = *((VertexId*) vertex_req.buf);
				vertex_req.rw = READ;
				vertex_req.size = p_iocb->u.c.nbytes;
				vertex_req.ptr_edge = p_iocb->data;
				assert(*((VertexId*)vertex_req.buf) == vertex);
				res.push_back(vertex_req);
			}
			aio.SubmitRequests();
		}
		return res;
	}
};

#endif //SAGE_VERTEX_CACHE_AIO_HPP
