#include "aio.hpp"

#include <vector>
#include <iostream>
#include <sys/time.h>
#include <unistd.h>
#include <cassert>
#include <random>
#include <algorithm>
#include "timer.hpp"

int main() {
	int fd;
	const int kPageSize = getpagesize();
	const size_t k1GB = 1024*1024*1024;
	const size_t kQueueDepth = 64;
	const int kSeqSize = kPageSize * 1024;
	const int kPageNum1GB = k1GB / kPageSize;
	struct timeval start, end;

	fd = open("/home/kckjn97/testfile", O_RDWR | O_CREAT | O_DIRECT, 644);
	if (fd < 0) {
		fprintf(stderr, "open error %d\n", fd);
		exit(-1);
	}

	uint8_t* w_buf;
	uint8_t* r_buf;

	/* MUST ALIGN for O_DIRECT */
	posix_memalign((void**) &w_buf, kPageSize, k1GB);
	posix_memalign((void**) &r_buf, kPageSize, k1GB);

	std::cout << "fill write_buffer\n";
	gettimeofday(&start, NULL);
	memset(w_buf, 0x00, k1GB);
	gettimeofday(&end, NULL);
	std::cout << "elapsed_time: " << TimeDiff(start, end) << "\n\n";

	Aio* aio = new Aio(kQueueDepth);

	std::cout << "sync sequential write\n";
	gettimeofday(&start, NULL);
	for(int i=0; i<k1GB/kSeqSize; i++)
		aio->WriteSync(fd, w_buf + i * kSeqSize, kSeqSize, i * kSeqSize);
	gettimeofday(&end, NULL);
	std::cout << "elapsed_time: " << TimeDiff(start, end) << "\n\n";

	std::cout << "sync sequential read\n";
	gettimeofday(&start, NULL);
	for(int i=0; i<k1GB/kSeqSize; i++)
		aio->ReadSync(fd, r_buf + i * kSeqSize, kSeqSize, i * kSeqSize);
	gettimeofday(&end, NULL);
	std::cout << "elapsed_time: " << TimeDiff(start, end) << "\n\n";

	std::cout << "check read_buffer\n";
	gettimeofday(&start, NULL);
	for(int i=0; i<k1GB; i++){
		assert(r_buf[i] == 0);
	}
	gettimeofday(&end, NULL);
	std::cout << "elapsed_time: " << TimeDiff(start, end) << "\n\n";


	std::cout << "shuffle\n";
	gettimeofday(&start, NULL);
	std::vector<int> vec;
	for(int i=0; i<kPageNum1GB; i++)
		vec.push_back(i);
	std::shuffle(vec.begin(), vec.end(), std::default_random_engine(0));
	gettimeofday(&end, NULL);
	std::cout << "elapsed_time: " << TimeDiff(start, end) << "\n\n";


	std::cout << "chagne write_buffer\n";
	gettimeofday(&start, NULL);
	memset(w_buf, 0xff, k1GB);
	gettimeofday(&end, NULL);
	std::cout << "elapsed_time: " << TimeDiff(start, end) << "\n\n";


	std::cout << "sync random write\n";
	gettimeofday(&start, NULL);
	for(int i=0; i<kPageNum1GB; i++) {
		unsigned int idx = vec[i];
		IoRequest req;
		req.fd = fd;
		req.offset = idx*kPageSize;
		req.rw = WRITE;
		req.buf = w_buf +idx*kPage
		aio->WriteSync(fd, w_buf + idx * kPageSize, kPageSize, idx * kPageSize);
	}
	gettimeofday(&end, NULL);
	std::cout << "elapsed_time: " << TimeDiff(start, end) << "\n\n";

	std::cout << "sync random read\n";
	gettimeofday(&start, NULL);
	for(int i=0; i<kPageNum1GB; i++) {
		unsigned int idx = vec[i];
		aio->ReadSync(fd, r_buf + idx * kPageSize, kPageSize, idx * kPageSize);
	}
	gettimeofday(&end, NULL);
	std::cout << "elapsed_time: " << TimeDiff(start, end) << "\n\n";

	std::cout << "check read_buffer\n";
	gettimeofday(&start, NULL);
	for(int i=0; i<k1GB; i++){
		assert(r_buf[i] == 0xff);
	}
	gettimeofday(&end, NULL);
	std::cout << "elapsed_time: " << TimeDiff(start, end) << "\n\n";

	std::cout << "change write_buffer\n";
	gettimeofday(&start, NULL);
	for(int i=0; i<k1GB; i++)
		w_buf[i] = (i >> 12) & 0xff;
	gettimeofday(&end, NULL);
	std::cout << "elapsed_time: " << TimeDiff(start, end) << "\n\n";

	std::cout << "async random write\n";
	gettimeofday(&start, NULL);
	for(int i=0; i<kPageNum1GB;) {
		assert(aio->RemainingQueueDepth());
		for(int j=0; j<aio->RemainingQueueDepth(); j++, i++) {
			unsigned int idx = vec[i];
			aio->WriteQueue(fd, w_buf + idx * kPageSize, kPageSize, idx * kPageSize);
		}

		if(aio->GetQueueSize())
			aio->SubmitRequests();

		std::pair<io_event*, int> _events = aio->WaitForComplete(1);
		_events = _events;
	}
	aio->WaitForAllComplete();
	gettimeofday(&end, NULL);
	std::cout << "elapsed_time: " << TimeDiff(start, end) << "\n\n";

	std::cout << "async random read\n";
	gettimeofday(&start, NULL);
	for(int i=0; i < kPageNum1GB;){
		assert(aio->RemainingQueueDepth());
		for(int j=0; j < aio->RemainingQueueDepth(); j++, i++) {
			unsigned int idx = vec[i];
			aio->ReadQueue(fd, r_buf + idx * kPageSize, kPageSize, idx * kPageSize, &vec[i]);
		}

		if(aio->GetQueueSize())
			aio->SubmitRequests();

		std::pair<io_event*, int> _events;
		if(i == kPageNum1GB - 1)
			_events = aio->WaitForAllComplete();
		else
			_events = aio->WaitForComplete(1);

		/* callback */
		for(int i=0; i<_events.second; i++){
			io_event* events = _events.first;
			iocb* p_iocb = events[i].obj;
			assert(p_iocb->aio_lio_opcode == IO_CMD_PREAD);
			long long off = p_iocb->u.c.offset;
			unsigned long nbytes = p_iocb->u.c.nbytes;
			uint8_t* buf = (uint8_t*) p_iocb->u.c.buf;

			printf("%x %d\n", p_iocb->data, *((int*) (p_iocb->data)));
			for(long long j=0; j<nbytes; j++){
				assert(buf[j] == (((off+j) >> 12) & 0xff));
			}
		}
	}
	gettimeofday(&end, NULL);
	std::cout << "elapsed_time: " << TimeDiff(start, end) << "\n\n";

	return 0;
}
