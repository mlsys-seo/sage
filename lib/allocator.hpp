#ifndef SAGE_ALLOCATOR_HPP
#define SAGE_ALLOCATOR_HPP

#include "bitmap.hpp"
#include <pthread.h>
#include "graph.hpp"

class DiskOffsetAllocator{
	off_t offset;
	pthread_spinlock_t lock;
public:
	DiskOffsetAllocator(){
		offset = 0;
		pthread_spin_init(&lock, 0);
	}

	off_t Alloc(size_t size){
		off_t ret;
		pthread_spin_lock(&lock);
		ret = offset;
		offset += size;
		pthread_spin_unlock(&lock);
		return ret;
	}
};

class VertexAllocator {
	Bitmap* bitmap;
	pthread_spinlock_t lock;
	unsigned int cursor;

public:
	VertexAllocator(Bitmap* bitmap)
			: cursor(0)
			, bitmap(bitmap){
		pthread_spin_init(&lock, 0);
	}

	std::vector<VertexId> GetVertices(unsigned int num=0){
		pthread_spin_lock(&lock);
		std::vector<off_t> tmp = bitmap->GetKeys(&cursor, num);
		pthread_spin_unlock(&lock);
		std::vector<VertexId> res(tmp.begin(), tmp.end());
		return res;
	}
};

template<typename Job>
class JobAllocator {
	std::vector<Job> jobs;
	pthread_spinlock_t lock;
	unsigned int cursor;

public:
	JobAllocator():
			cursor(0){
	}

	void AddJob(Job job){
		pthread_spin_lock(&lock);
		jobs.push_back(job);
		pthread_spin_unlock(&lock);
	}

	void AddJobs(Job* _jobs, unsigned int num){
		pthread_spin_lock(&lock);
		jobs.reserve(jobs.size() + num);
		copy(&_jobs[0], &_jobs[num], back_inserter(jobs));
		pthread_spin_unlock(&lock);
	}

	std::pair<Job*, unsigned int> GetJobs(unsigned int num){
		Job* ptrJobs;
		if(cursor >= jobs.size()){
			ptrJobs = NULL;
			return std::make_pair(ptrJobs, 0);
		}
		pthread_spin_lock(&lock);
		ptrJobs =  &(jobs.data()[cursor]);
		if(cursor+num > jobs.size())
			num = jobs.size() - cursor;
		cursor += num;
		pthread_spin_unlock(&lock);
		return std::make_pair(ptrJobs, num);
	}

	void ClearJobs(){
		jobs.clear();
		cursor = 0;
	}
};
#endif //SAGE_ALLOCATOR_HPP
