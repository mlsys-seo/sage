#include "thread.hpp"
#include <iostream>
#include <vector>
#include <unistd.h>
#include <mutex>
#include "allocator.hpp"
#include "timer.hpp"
#include <fcntl.h>

size_t vertex_size;
size_t vertex_num;
size_t sample_num;

int fd;

int num_worker = 14;

class Worker: public Thread {
	int wid;
public:
	Worker(unsigned int wid)
			: Thread(wid){
		this->wid = wid;
		std::cout << wid << " worker init\n";
	}

	void Run() {
		long a = 0;
		int fd = open("tmp", O_RDONLY, 655);
		void* buf = malloc(vertex_size);
		memset(buf,0xff, vertex_size);
		pread(fd, buf, 8, 0);
		size_t off_vertex = vertex_num / num_worker * wid;
		for(int j=off_vertex; j<off_vertex+vertex_num/num_worker; j++) {
			for (int i = 0; i < sample_num; i++) {
				off_t vertex_id = j;
				off_t sample = i;
				size_t graph_size = vertex_num * vertex_size;
				off_t off = graph_size * sample + vertex_id * vertex_size;
				int ret = pread(fd, buf, vertex_size, off);
				a += *((int*)buf);
//				printf("%d %d %d\n", errno, off, *((int*)buf));
			}
		}
		printf("%ld\n", a);
	}
};

int main(int argc, char** argv) {
	vertex_size = atoi(argv[1]);
	vertex_num = atoi(argv[2]);
	sample_num = atoi(argv[3]);

	fd = open("tmp", O_RDWR | O_CREAT, 655);
	size_t file_size = vertex_size * vertex_num * sample_num;

	void *buf = malloc(file_size);
	printf("%d written\n", file_size >> 20);
	memset(buf, 0x0f, file_size);
	printf("%d \n", ((int*)buf)[0]);
	pwrite(fd,  buf, file_size, 0);
	pread(fd,  buf, 8, 0);
	printf("%d \n", ((int*)buf)[0]);
	free(buf);
	close(fd);

	struct timeval start, end;
	gettimeofday(&start, 0);
	std::vector<Worker> workers;

	for(int i=0; i<num_worker; i++) {
		workers.push_back(Worker(i));
	}

	for(int i=0; i<num_worker; i++)
		workers[i].Start();

	for(int i=0; i<num_worker; i++)
		workers[i].Join();
	gettimeofday(&end, 0);

	float a = TimeDiff(start, end);
	printf("time: %f\n", a);
}