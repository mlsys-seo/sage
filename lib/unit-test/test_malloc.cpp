#include <cstdlib>
#include "timer.hpp"
#include <iostream>
#include <cstring>

int main(){
	int sum = 0;
	struct timeval start, end;
	gettimeofday(&start, 0);
	uint64_t* a = (uint64_t *) malloc(sizeof(uint64_t)*16 *100000000);
	memset(a, 0xff, sizeof(uint64_t) * 16*100000000);
	for(int i=0; i<100000000; i++){
//		uint64_t* a = new uint64_t[16];
//		sum += a[i%16+i*16];
	}
	free(a);
	gettimeofday(&end, 0);
	printf("%d %f\n", sum, TimeDiff(start, end));

	gettimeofday(&start, 0);

	uint64_t **b;
	b = (uint64_t **) malloc(sizeof(uint64_t*) * 100000000);
	for(int i=0; i<100000000; i++) {
		b[i] = (uint64_t *) malloc(sizeof(uint64_t) * 16);
		memset(b[i], 0xff, sizeof(uint64_t) * 16);
//		sum += b[i][i%16];
		free(b[i]);
	}
	free(b);
	gettimeofday(&end, 0);
	printf("%d %f\n", sum, TimeDiff(start, end));
}