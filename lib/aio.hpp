#ifndef SAGE_AIO_HPP
#define SAGE_AIO_HPP

#include <libaio.h>
#include <list>
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <vector>
#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include <queue>
#include "config.hpp"


#define IO_LOG 0


//#define PAGE_SIZE 512UL

#define WRITE 0
#define READ 1
#define READ_INIT_VALUE 2

struct IoRequest{
	int fd;
	off_t offset;
	size_t size;
	uint8_t* buf;
	uint8_t rw;
	void* callback;
};

class Aio {
	io_context_t io_ctx;
	size_t queue_depth;
	size_t num_submit_requests;
	size_t num_complete_requests;
	std::list<IoRequest> queued_requests;
	iocb *arr_iocb;
	std::vector<int> empty_iocb;
	size_t cnt_read;
	size_t cnt_write;
	size_t amount_read;
	size_t amount_write;

public:
	Aio(size_t _queue_depth)
			: io_ctx(0), queue_depth(_queue_depth)
			, num_submit_requests(0)
			, num_complete_requests(0) {
		int err = io_setup(queue_depth, &io_ctx);
		if (err) {
			fprintf(stderr, "io_setup failed : %d\n", -err);
			exit(-1);
		}
		arr_iocb = new iocb[queue_depth];
		empty_iocb.reserve(queue_depth);
		for(int i=0; i< queue_depth;i++)
			empty_iocb.push_back(i);
		InitStat();
	}

	~Aio() {
		int err = io_destroy(io_ctx);
		if (err) {
			fprintf(stderr, "io_destroy failed : %d\n", -err);
			exit(-1);
		}
	}

	void InitStat(){
		cnt_read = cnt_write = 0;
		amount_read = amount_write = 0;
	}

	void PrintStat(){
		printf("read: %lu, %lu,\t write: %lu, %lu\n", amount_read, cnt_read, amount_write, cnt_write);
	}

	inline size_t GetQueueSize(){
		return queued_requests.size();
	}

	inline size_t GetNumWaitingRequests() {
		return num_submit_requests - num_complete_requests;
	}

	inline size_t RemainingQueueDepth() {
		return queue_depth - (num_submit_requests - num_complete_requests);
	}

	inline void AsyncRequest(IoRequest io_request){
		assert(io_request.offset != ~0UL);
		queued_requests.push_back(io_request);
	}

	inline void SyncRequest(IoRequest io_request){
		size_t ret;
		if(io_request.rw == READ)
			ret = pread(io_request.fd, io_request.buf, io_request.size, io_request.offset);
		else if(io_request.rw == WRITE)
			ret = pwrite(io_request.fd, io_request.buf, io_request.size, io_request.offset);

		if(ret < 0)
			fprintf(stderr, "sync_request : %d\n", ret);
	}

	size_t SubmitRequests() {
		size_t request_count = std::min(queued_requests.size(), RemainingQueueDepth());
		if(request_count == 0)
			return 0;

		std::vector<iocb *> requests;
		requests.reserve(request_count);

		int n=0;
		for (auto it = queued_requests.begin(); it != queued_requests.end() && n <request_count; ++it, n++) {
			assert(empty_iocb.size() > 0);
			int idx = empty_iocb.back();
			empty_iocb.pop_back();

			iocb *p_iocb = &arr_iocb[idx];


			if (it->rw == READ)
				io_prep_pread(p_iocb, it->fd, it->buf, it->size, it->offset);
			else if (it->rw == WRITE)
				io_prep_pwrite(p_iocb, it->fd, it->buf, it->size, it->offset);
			p_iocb->data = it->callback;
#if IO_LOG
			if (p_iocb->aio_lio_opcode == IO_CMD_PREAD) {
				*((int *) p_iocb->u.c.buf) = 11;
			}
			printf("io: offset %lu, rw: %d, %d\n", p_iocb->u.c.offset, p_iocb->aio_lio_opcode, *((int*)p_iocb->u.c.buf));
#endif
			requests.push_back(p_iocb);
		}

		// Submit all requests.
		int err = io_submit(io_ctx, request_count, requests.data());
		if (err < 0) {
			for(int i=0; i<request_count; i++){

				printf("fd: %d, offset: %lu, size: %u, buf: %p\n", requests[i]->aio_fildes, requests[i]->u.c.offset, requests[i]->u.c.nbytes, requests[i]->u.c.buf);
			}
			fprintf(stderr, "io_submit failed : %d\n", -err);
			exit(-1);
		}

		assert(err == request_count);
		assert(queued_requests.size() >= request_count);

		queued_requests.erase(queued_requests.begin(), std::next(queued_requests.begin(), request_count));
		num_submit_requests += request_count;
		return request_count;
	};

	std::vector<io_event> WaitForComplete( size_t min_requests, size_t max_requests = 128 ) {
		std::vector<io_event> res;
		assert(max_requests >= min_requests);

		io_event events[ max_requests ];
		int event_count = io_getevents( io_ctx, min_requests, max_requests, events, nullptr );
		if( event_count < 0 )
		{
			fprintf(stderr, "io_getevents failed: %d\n", event_count);
			exit(-1);
		}
#if 0
		for(int i=0; i<event_count; i++) {
			iocb *p_iocb = events[i].obj;
			if(p_iocb->aio_lio_opcode == IO_CMD_PREAD) {


				IoRequest io_req;
				io_req.offset = p_iocb->u.c.offset;
				io_req.size = p_iocb->u.c.nbytes;
				io_req.fd = p_iocb->aio_fildes;
				io_req.rw = READ;
				posix_memalign((void **) &io_req.buf, PAGE_SIZE, PAGE_SIZE);
				SyncRequest(io_req);

				printf("read miss check %d:%d\n", *((int *) io_req.buf), *((int *) p_iocb->u.c.buf));
				assert(*((int *) p_iocb->u.c.buf) == *((int *) io_req.buf));
				free(io_req.buf);
			}
		}
#endif
		res.reserve(event_count);
		for(int i=0; i<event_count; i++) {
			res.push_back(events[i]);
			int idx = events[i].obj - arr_iocb;
			empty_iocb.push_back(idx);
		}

		assert( event_count >= min_requests );
		assert( event_count <= max_requests );
		num_complete_requests += event_count;

		return res;
	}

	inline std::vector<io_event> WaitForAllComplete() {
		return WaitForComplete(num_submit_requests - num_complete_requests);
	}
};

#endif //SAGE_AIO_HPP
