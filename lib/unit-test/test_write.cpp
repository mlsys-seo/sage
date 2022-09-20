#include <unordered_map>
#include <boost/dynamic_bitset.hpp>
//#include "simple_lru.hpp"
#include "priority_queue.hpp"
#include <queue>
#include <cstring>
#include "thread.hpp"
#include <random>
#include <cstdio>
#include "aio.hpp"
#include <iostream>

class DirtyThread: public Thread{
	std::unordered_map <off_t, void*> map_dirty_pending;
	pthread_spinlock_t lock;
	size_t blk_size;
	size_t max_blk;
	Aio aio;
	bool running;
	int fd;
	size_t num_writing;

public:
	DirtyThread(int fd, size_t blk_size=4096, size_t max_blk = 512):
			fd(fd),
			aio(max_blk),
			max_blk(max_blk),
			blk_size(blk_size){
		pthread_spin_init(&lock, 0);
		num_writing = 0;
	}

	void Exit(){
		pthread_spin_unlock(&lock);
		running = false;
	}

	void* AsyncWrite(off_t off, void* buf, size_t size = PAGE_SIZE){
		void* _buf;
		posix_memalign(&_buf, blk_size, blk_size);
		memcpy(_buf, buf, blk_size);
		struct IoRequest io_request;
		io_request.buf = (uint8_t*) _buf;
		io_request.size = size;
		io_request.rw = WRITE;
		io_request.fd = fd;
		io_request.callback = NULL;
		io_request.offset = off;
//		printf("%d %d %d\n", io_request.fd, io_request.offset, io_request.size  );
		pthread_spin_lock(&lock);
		map_dirty_pending.insert({off, _buf});
		aio.AsyncRequest(io_request);
		num_writing++;
		pthread_spin_unlock(&lock);
		return _buf;
	}

	int Find(off_t off, void* buf){
		pthread_spin_lock(&lock);
		auto it = map_dirty_pending.find(off);
		if(it == map_dirty_pending.end()) {
			pthread_spin_unlock(&lock);
			return 0;
		}
		memcpy(buf, it->second, blk_size);
		pthread_spin_unlock(&lock);
		return 1;
	}

	size_t GetNumWriting(){
		return num_writing;
	}

	void Run(){
		size_t max= 0;
		running = true;
		while(running){
			pthread_spin_lock(&lock);
			aio.SubmitRequests();
			pthread_spin_unlock(&lock);
			if(aio.GetNumWaitingRequests()){
				std::vector<io_event> events = aio.WaitForComplete(1);

				/* callback */
				for(int i=0; i<events.size(); i++){
					uint8_t* buf = (uint8_t*) events[i].obj->u.c.buf;
					assert(events[i].obj->aio_lio_opcode == IO_CMD_PWRITE);
					assert(events[i].obj->u.c.nbytes == blk_size);

					pthread_spin_lock(&lock);
					map_dirty_pending.erase((off_t) events[i].obj->u.c.offset);
					num_writing--;
					pthread_spin_unlock(&lock);
					free(buf);
				}
			}
//			usleep(1);
		}
	}
};


int main(int argc, char** argv){
	int fd = open(, O_RDWR| O_CREAT | O_DIRECT, 0644);
	if (fd < 0) {
		fprintf(stderr, "not exist %d\n", fd);
		exit(-1);
	}
	DirtyThread dirty_thread(fd, 4096);
	dirty_thread.Start();

	std::random_device rd;
	std::default_random_engine random_engine(rd());
	std::uniform_int_distribution<int> uniform_dist(0, 1024*128);
	printf("fill\n");
	for(int i=0; i<1024*128; i++) {
		uint8_t buf[4096];
		dirty_thread.AsyncWrite(i*4096, buf, 4096);
	}
	printf("write\n");
	for(int i=0; i<1024*1024*1024; i++){
		float rnd = uniform_dist(random_engine);
		uint8_t buf[4096];
		dirty_thread.AsyncWrite(rnd*1024, buf, 4096);
	}
}