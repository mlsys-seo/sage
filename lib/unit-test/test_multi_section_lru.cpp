#include "multi_section_lru.hpp"
#include "assert.h"
#include <random>

int main(int argc, char** argv) {
	std::random_device rd;
	std::default_random_engine random_engine(rd());
	std::uniform_int_distribution<int> uniform_dist(2, 4);

	MultiSectionLru msl(5, 100);
	for(int i=0; i<120; i++) {
		int rnd = uniform_dist(random_engine);
		printf("%d section add\n", rnd);
		off_t a = msl.GetVictim(rnd);
		msl.Access(a, rnd);
		msl.Print();
		printf("\n");
	}

	return 0;
};
