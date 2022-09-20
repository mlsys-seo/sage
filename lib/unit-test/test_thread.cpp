#include "thread.hpp"
#include <iostream>
#include <vector>
#include <unistd.h>
#include <mutex>
#include "allocator.hpp"

typedef int Job;

class Worker: public Thread {
	unsigned int wid;
	std::vector<Job> jobs;
	unsigned int part_size;
	JobAllocator<Job>* ptr_job_allocator;

public:
	Worker(unsigned int wid, unsigned int part_size, JobAllocator<Job>* ptr_job_allocator)
	: Thread(wid)
	, wid(wid)
	, part_size(part_size)
	, ptr_job_allocator(ptr_job_allocator) {
		std::cout << wid << " worker init\n";
	}

	void AddJobs(Job* _jobs, unsigned int num) {
		jobs.reserve(jobs.size() + num);
		copy(&_jobs[0], &_jobs[num], back_inserter(jobs));
	}

	void Run() {
		std::pair<Job *, unsigned int> allocated_job;
		allocated_job = ptr_job_allocator->GetJobs(part_size);

		do {
			AddJobs(allocated_job.first, allocated_job.second);
			for (int i = 0; i < part_size; i++) {
				std::cout << wid << ": " << jobs[i] << std::endl;
//				usleep(10);
			}
			jobs.clear();
			allocated_job = ptr_job_allocator->GetJobs(part_size);
		}while(allocated_job.second);

		std::cout << wid << " worker end\n";
	}
};

int main() {
	const int num_total_job = 10000;
	const int num_worker = 8;
	const int part_size = 10;

	std::vector<Job> jobs;
	jobs.reserve(num_total_job);
	JobAllocator<Job> job_allocator;

	for(int i=0; i<num_total_job; i++)
		jobs.push_back(i);
	job_allocator.AddJobs(jobs.data(), jobs.size());

	std::vector<Worker> workers;
	for(int i=0; i<num_worker; i++) {
		workers.push_back(Worker(i, part_size, &job_allocator));
	}

	for(int i=0; i<num_worker; i++)
		workers[i].Start();

	for(int i=0; i<num_worker; i++)
		workers[i].Join();
}