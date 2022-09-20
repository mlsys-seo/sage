#include <iostream>
#include <edge_sampling.hpp>
#include <assert.h>

int main() {
	unsigned int src = 1;
	unsigned int dest = 2;
	unsigned int num_sample = 32;
	float prob = 0.5;

	EdgeSampling edge_sampling1 = EdgeSampling(src, dest, num_sample, prob);
	edge_sampling1.GetBitmap()->Print();

	EdgeSampling edge_sampling2 = EdgeSampling(src, dest, num_sample, prob);
	edge_sampling2.GetBitmap()->Print();

	EdgeSampling edge_sampling3 = EdgeSampling(dest, src, num_sample, prob);
	edge_sampling3.GetBitmap()->Print();

	assert(*edge_sampling1.GetBitmap() == *edge_sampling2.GetBitmap());
	assert(*edge_sampling3.GetBitmap() != *edge_sampling2.GetBitmap());

	std::cout << "Success\n";
	return 0;
}