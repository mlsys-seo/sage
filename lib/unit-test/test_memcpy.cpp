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
	struct timeval start, end;

	const int kPages = 1;
	uint8_t *w_buf;
	uint8_t *r_buf;

	/* MUST ALIGN for O_DIRECT */
	posix_memalign((void **) &w_buf, kPageSize, kPageSize*kPages);
	posix_memalign((void **) &r_buf, kPageSize, kPageSize*kPages);

	gettimeofday(&start, NULL);
	for(unsigned long i=0; i<21000000UL*4UL; i++) {
		*((unsigned long*)r_buf) = i;
		memcpy(w_buf, r_buf, kPageSize * kPages);
		assert(*((unsigned long*) w_buf) == i);
	}
	gettimeofday(&end, NULL);
	std::cout << "elapsed_time: " << TimeDiff(start, end) << "\n\n";
}