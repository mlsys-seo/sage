#ifndef SAGE_VERTEX_CACHE_HPP
#define SAGE_VERTEX_CACHE_HPP

#include "vertex.hpp"
#include <boost/dynamic_bitset.hpp>
#include <queue>
#include <cstring>
#include "thread.hpp"
#include "config.hpp"
#include "multi_section_lru.hpp"
#include "aio.hpp"
#include <algorithm>
#include <atomic>
#include "serialized_value_bitmap.hpp"

#define CACHE_MISS      0
#define CACHE_HIT       1
#define CACHE_ERR      -1

#define CACHE_LOG  0

class VertexCache {
	size_t allocated_offset;

	size_t num_vertex;
	size_t vertex_init_size;
	size_t max_cache_size;
	void* init_value_buf;

	struct VertexCacheEntry{
		void* buf;
		void* to_updated_buf;
		size_t to_updated_size;
		off_t offset;
		size_t size;
		unsigned int use_cnt;
	};

	pthread_spinlock_t* entry_lock;
	struct VertexCacheEntry* entry;
	int fd;
	std::atomic<size_t> current_size;
	std::atomic<size_t> hits;
	std::atomic<size_t> misses;
	std::atomic<size_t> read_bytes;
	std::atomic<size_t> write_bytes;
	Aio aio;

	MultiSectionLru lru;
	boost::dynamic_bitset<> is_dirty;
	pthread_spinlock_t lock;
	int current_section;

public:
	VertexCache(int fd, size_t max_cache_size, size_t num_vertex, size_t vertex_init_size, void* init_value_buf, size_t num_section): aio(16), lru(num_vertex, num_section), is_dirty(num_vertex) {
		printf("allocated cache memory (MB): %.1f\n", max_cache_size/1024.0/1024);
		this->fd = fd;
		this->max_cache_size = max_cache_size;
		this->num_vertex = num_vertex;
		this->vertex_init_size = vertex_init_size;
		this->init_value_buf = init_value_buf;

		entry = new VertexCacheEntry[num_vertex];
		entry_lock = new pthread_spinlock_t[num_vertex];
		lru.SetVertexLock(entry_lock);

		Init();

		hits = 0;
		misses = 0;
		write_bytes = 0;
		read_bytes = 0;
		allocated_offset = 0;
		current_size = 0;
		pthread_spin_init(&lock, 0);
		current_section = 2;
	}

	~VertexCache(){
		delete[] entry;
		delete[] entry_lock;
	}

	void Init(){
		lru.Init();
		current_section = 2;
		current_size = 0;
		for(int i=0; i<num_vertex; i++){
			free(entry[i].buf);
			entry[i].buf = NULL;
			entry[i].to_updated_buf = NULL;
			entry[i].to_updated_size = 0;
			entry[i].offset = ~0UL;
			entry[i].size = vertex_init_size;
			entry[i].use_cnt = 0;
			pthread_spin_init(&entry_lock[i], 0);
		}
	}

	void SetCurrentSection(int s){
		pthread_spin_lock(&lock);
		lru.Cleaning(current_section, s);
		current_section = s;
		pthread_spin_unlock(&lock);
	}

	void PrintStat(int i){
		if(i == 0) {
			printf("cache hit ratio: %f\n", hits / ((float) hits + misses));
			printf("cache hits: %lu\n", hits.load());
			printf("cache misses: %lu\n", misses.load());
			printf("used cache memory: %.1f\n", current_size/1024.0/1024);
			printf("read MB: %.1f\n", read_bytes/1024.0/1024);
			printf("write MB: %.1f\n", write_bytes/1024.0/1024);
		}else
			printf("\t%f, (%u, %u), %.1f MB, %.1f MB Read, %.1f MB Write\n", hits/((float)hits+misses), hits.load(), misses.load(), current_size/1024.0/1024, read_bytes/1024.0/1024, write_bytes/1024.0/1024);

	}

	void StatInit(){
		hits = misses = 0;
	}

	void Flush(VertexId vid){
		assert(entry[vid].use_cnt == 0);
		if(entry[vid].to_updated_size != 0 || entry[vid].to_updated_buf != NULL){
			printf("%d %p\n", entry[vid].to_updated_size, entry[vid].to_updated_buf);
		};
		assert(entry[vid].to_updated_size == 0 && entry[vid].to_updated_buf == NULL);
		if(entry[vid].offset == ~0UL) {
			entry[vid].offset = allocated_offset;
			allocated_offset += entry[vid].size;
		}
		assert(entry[vid].use_cnt == 0);
		assert(vid == *((VertexId*) entry[vid].buf));

		IoRequest io_req;
		io_req.buf = (uint8_t *) entry[vid].buf;
		io_req.offset = entry[vid].offset;
		io_req.fd = fd;
		io_req.size = entry[vid].size;
		io_req.rw = WRITE;
		assert(*((VertexId*) io_req.buf) == vid);

		write_bytes.fetch_add(io_req.size);

		aio.SyncRequest(io_req);
	}

	void Replace(VertexId vid, size_t size){
		size_t cache_size = current_size + size;
		VertexId victim = ~0U;
		while(cache_size > max_cache_size){
			victim = lru.GetVictim(current_section);
			while(victim == ~0U){
				assert(0);
				pthread_spin_unlock(&lock);
				usleep(100);
				printf("sleep\n");
				pthread_spin_lock(&lock);
				VertexId victim = lru.GetVictim(current_section);
			}
			pthread_spin_lock(&entry_lock[victim]);
			if(vid == victim) {
				lru.Access(victim, 1, 0, false);
				pthread_spin_unlock(&entry_lock[victim]);
				continue;
			}
#if CACHE_LOG
			printf("victim: %d for %d\n",victim, vid);
#endif
			int use_cnt = entry[victim].use_cnt;
			int a = 0;
			while(use_cnt != 0 ){
				pthread_spin_unlock(&lock);
				pthread_spin_unlock(&entry_lock[victim]);
				usleep(1);
				pthread_spin_lock(&lock);
				pthread_spin_lock(&entry_lock[victim]);
				lru.Access(victim, 1, 0, false);
				pthread_spin_unlock(&entry_lock[victim]);
				victim = lru.GetVictim(current_section);
				pthread_spin_lock(&entry_lock[victim]);
				use_cnt = entry[victim].use_cnt;
			}

			if (entry[victim].buf != NULL) {
				if (is_dirty[victim])
					Flush(victim);

				assert(*((VertexId *) entry[victim].buf) == victim);
				assert(entry[victim].to_updated_buf == NULL);
				assert(entry[victim].to_updated_size == 0);
				free(entry[victim].buf);
				entry[victim].buf = NULL;
				entry[victim].to_updated_buf = NULL;
				entry[victim].to_updated_size = 0;
				is_dirty[victim] = false;

				current_size.fetch_sub(entry[victim].size);
				cache_size = current_size + size;
			} else {
				assert(is_dirty[victim] == false);
			}
			pthread_spin_unlock(&entry_lock[victim]);
			return;
		}
	}

	void Free(void* buf, size_t size){
		free(buf);
		read_bytes.fetch_sub(size);
	}

	//buf 포인터만 주고, use_cnt  늘리고, copy할지말지는 윗단에서 결정
	int Get(VertexId vid, void** ptr_buf, size_t* ptr_size, IoRequest* io_req, bool retry = false){
		pthread_spin_lock(&entry_lock[vid]);
		if(entry[vid].offset == ~0UL && entry[vid].buf == 0) {
			if(!retry)
				hits.fetch_add(1);
			assert(*ptr_buf == 0);
			*ptr_buf = init_value_buf;
			*ptr_size = vertex_init_size;
#if CACHE_LOG
			printf("vid: %d(%d) init copy\n", vid, entry[vid].use_cnt);
#endif
//			pthread_spin_unlock(&lock);
			pthread_spin_unlock(&entry_lock[vid]);
			return CACHE_HIT;
		}

		//miss
		if(entry[vid].buf == 0) {
			if(!retry)
				misses.fetch_add(1);
			assert(io_req != NULL);
			entry[vid].use_cnt++;
			io_req->offset = entry[vid].offset;
			io_req->size = entry[vid].size;
			io_req->rw = READ;
			io_req->fd = fd;
			posix_memalign((void**) &io_req->buf, PAGE_SIZE, io_req->size);
			read_bytes.fetch_add(io_req->size);
#if CACHE_LOG
			printf("vid: %d (%d) get miss\n", vid, entry[vid].use_cnt);
#endif
			pthread_spin_unlock(&entry_lock[vid]);
			return CACHE_MISS;
		}
		entry[vid].use_cnt++;
		assert(*((VertexId*) entry[vid].buf) == vid);
		//hit
		if(!retry)
			hits.fetch_add(1);
		*ptr_buf = entry[vid].buf;
		*ptr_size = entry[vid].size;
		assert(*ptr_size >= PAGE_SIZE);
		assert(*ptr_buf != NULL);

#if CACHE_LOG
		printf("vid: %d (%d) get hit\n", vid, entry[vid].use_cnt);
#endif
		lru.Access(vid, -1, 0, true);
		pthread_spin_unlock(&entry_lock[vid]);
		return CACHE_HIT;
	}

	int Put(VertexId vid, void** ptr_buf, size_t* ptr_size, int section = 0, double priority = 0, bool for_read_cache = false){
		//miss
		int ret = CACHE_HIT;
		pthread_spin_lock(&lock);
		pthread_spin_lock(&entry_lock[vid]);

#if CACHE_LOG
		printf("put: %d (%d)\n", vid, entry[vid].use_cnt);
#endif

		assert(*((VertexId*) *ptr_buf) == vid);
		// 캐시에 있는것을 그대로 쓴 경우
		if(entry[vid].buf == *ptr_buf && entry[vid].size == *ptr_size) {
			assert(0);
			pthread_spin_unlock(&lock);
			pthread_spin_unlock(&entry_lock[vid]);
			return ret;
		}

		if(for_read_cache == false && entry[vid].size != *ptr_size){
			entry[vid].offset = ~0UL;
		}

		//cache miss
		if(entry[vid].buf == 0) {
			if (!for_read_cache)
				misses.fetch_add(1);
			pthread_spin_unlock(&entry_lock[vid]);
			Replace(vid, *ptr_size);
			pthread_spin_lock(&entry_lock[vid]);

			ret = CACHE_MISS;
			current_size.fetch_add(*ptr_size);
			if (for_read_cache == true) {
				assert(entry[vid].use_cnt != 0);
				lru.Access(vid, 1, 0, true);
				entry[vid].buf = *ptr_buf;
				assert(entry[vid].size == *ptr_size);
				pthread_spin_unlock(&lock);
				pthread_spin_unlock(&entry_lock[vid]);

				return ret;
			}
		}else if(for_read_cache == true) {
			// read miss write -> cache hit
			assert(entry[vid].use_cnt != 0);
			assert(*ptr_buf != entry[vid].buf);
			assert(*((VertexId*) entry[vid].buf) == vid);
			assert(*((VertexId*) (*ptr_buf)) == vid);
			free(*ptr_buf);
			*ptr_buf = entry[vid].buf;
			*ptr_size = entry[vid].size;
			lru.Access(vid, 1, 0, false);

//			Check(vid);
			pthread_spin_unlock(&lock);
			pthread_spin_unlock(&entry_lock[vid]);

			return ret;
		}else if(entry[vid].size != *ptr_size) {
			//hit 이지만 size 다른 경우

			assert(*ptr_size >  entry[vid].size);
			pthread_spin_unlock(&entry_lock[vid]);
			size_t tmp_size = (*ptr_size) - entry[vid].size; 
			Replace(vid, tmp_size);

			pthread_spin_lock(&entry_lock[vid]);

			hits.fetch_add(1);

			current_size.fetch_add(tmp_size);

			is_dirty[vid] |= true;

			assert(*((VertexId*) *ptr_buf) == vid);

			if (entry[vid].use_cnt == 0) {
				lru.Access(vid, section, priority, true);
				entry[vid].size = *ptr_size;
				entry[vid].offset = ~0UL;
				free(entry[vid].buf);
				entry[vid].buf = *ptr_buf;
				assert(entry[vid].to_updated_buf == NULL);
				assert(entry[vid].to_updated_size == 0);
			}else{
				lru.Access(vid, -1, 0, true);
				entry[vid].to_updated_size = *ptr_size;
				entry[vid].to_updated_buf = *ptr_buf;
			}

			pthread_spin_unlock(&lock);
			pthread_spin_unlock(&entry_lock[vid]);
			return ret;
		}

		hits.fetch_add(1);

		// 기존 삭제
		if(entry[vid].buf != *ptr_buf && entry[vid].use_cnt == 0) {
			assert(entry[vid].to_updated_buf == NULL);
			free(entry[vid].buf);
		}

		assert(for_read_cache == false);
		is_dirty[vid] |= true;

		assert(*((VertexId*) *ptr_buf) == vid);

		if(entry[vid].use_cnt == 0)
			lru.Access(vid, section, priority, true);
		else
			lru.Access(vid, -1, 0, true);

		if (entry[vid].use_cnt == 0) {
			entry[vid].buf = *ptr_buf;
			entry[vid].size = *ptr_size;
			assert(entry[vid].to_updated_buf == NULL);
			assert(entry[vid].to_updated_size == 0);
		} else {
			entry[vid].to_updated_buf = *ptr_buf;
			entry[vid].to_updated_size = *ptr_size;
		}

//		Check(vid);
		pthread_spin_unlock(&lock);
		pthread_spin_unlock(&entry_lock[vid]);
		return ret;
	}

	void EndRead(VertexId vertex, void* buf, int section, double priority){
		if(buf != init_value_buf) {
			pthread_spin_lock(&entry_lock[vertex]);
#if CACHE_LOG
			printf("end_read: %d (%d)\n", vertex, entry[vertex].use_cnt);
#endif
			assert(entry[vertex].use_cnt > 0);
			entry[vertex].use_cnt--;
			if(entry[vertex].use_cnt == 0) {
				lru.Access(vertex, section, priority, false);

				if(entry[vertex].to_updated_buf != NULL) {
					free(entry[vertex].buf);

					assert(entry[vertex].to_updated_size != 0);
					if(entry[vertex].size != entry[vertex].to_updated_size){
						entry[vertex].offset = ~0UL;
						entry[vertex].size = entry[vertex].to_updated_size;
					}
					entry[vertex].buf = entry[vertex].to_updated_buf;
					entry[vertex].to_updated_buf = NULL;
					entry[vertex].to_updated_size = 0;
				}
			}else{
				lru.Access(vertex, 1, 0, false);
			}
			pthread_spin_unlock(&entry_lock[vertex]);
		}
	}
};
#endif //SAGE_VERTEX_CACHE_HPP
