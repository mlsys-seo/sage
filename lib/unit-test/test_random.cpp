#include <random>
#include <iostream>
#include <sys/time.h>
#include "graph.hpp"
#include "timer.hpp"
#include <algorithm>
#include "sha256.h"
#include "crc32.h"
#include "md5.h"
#include <stdlib.h>

static inline uint32_t murmur_32_scramble(uint32_t k) {
	k *= 0xcc9e2d51;
	k = (k << 15) | (k >> 17);
	k *= 0x1b873593;
	return k;
}

uint32_t murmur3_32(const uint8_t* key, size_t len, uint32_t seed)
{
	uint32_t h = seed;
	uint32_t k;
	/* Read in groups of 4. */
	for (size_t i = len >> 2; i; i--) {
		// Here is a source of differing results across endiannesses.
		// A swap here has no effects on hash properties though.
		memcpy(&k, key, sizeof(uint32_t));
		key += sizeof(uint32_t);
		h ^= murmur_32_scramble(k);
		h = (h << 13) | (h >> 19);
		h = h * 5 + 0xe6546b64;
	}
	/* Read the rest. */
	k = 0;
	for (size_t i = len & 3; i; i--) {
		k <<= 8;
		k |= key[i - 1];
	}
	// A swap is *not* necessary here because the preceding loop already
	// places the low bytes in the low places according to whatever endianness
	// we use. Swaps only apply when the memory is copied in a chunk.
	h ^= murmur_32_scramble(k);
	/* Finalize. */
	h ^= len;
	h ^= h >> 16;
	h *= 0x85ebca6b;
	h ^= h >> 13;
	h *= 0xc2b2ae35;
	h ^= h >> 16;
	return h;
}

void make_random(Graph* graph, int num_sample, FILE* fp_output, int hash){
	const unsigned int kPrime = 100000007;
	// create a new hashing object
	SHA256 sha256;
	MD5 md5;
	CRC32 crc32;

	// hashing an std::string


	// or in a streaming fashion (re-use "How are you")
	std::random_device rd;
	std::default_random_engine random_engine(rd());
	std::uniform_real_distribution<float> uniform_dist(0.0, 1.0);
//	std::uniform_int_distribution<int> uniform_dist(0, 1000);

	unsigned int seed = 0;
	random_engine.seed(seed);

	unsigned long* n = new unsigned long[1000001];
	struct timeval start, end;
	gettimeofday(&start, 0);
	for(unsigned int i=0; i<graph->GetMaxVertexId()+1; i++) {
		EdgeList edge_list = graph->GetOutEdgeList(i);
		for(unsigned int j=0; j<edge_list.GetDegree(); j++) {
			Edge edge = edge_list.GetEdge(j);
			VertexId src = i;
			VertexId dest = edge.neighbor;
			if(hash == 1)
				seed = dest * kPrime + src;
			else if(hash == 2)
				seed = murmur3_32((uint8_t*) &dest, sizeof(dest), 0) +  murmur3_32((uint8_t*) &src, sizeof(src), 0);
			else if(hash == 3){
				std::string str_hash_dest = sha256(std::to_string(dest));
				unsigned int hash_dest = strtol(str_hash_dest.substr(0,8).c_str(), NULL, 16);
				std::string str_hash_src = sha256(std::to_string(src));
				unsigned int hash_src = strtol(str_hash_src.substr(0,8).c_str(), NULL, 16);
				seed = hash_dest + hash_src;
			}else if(hash == 4){
				std::string str_hash_dest = md5(std::to_string(dest));
				unsigned int hash_dest = strtol(str_hash_dest.substr(0,8).c_str(), NULL, 16);
				std::string str_hash_src = md5(std::to_string(src));
				unsigned int hash_src = strtol(str_hash_src.substr(0,8).c_str(), NULL, 16);
				seed = hash_dest + hash_src;
			} else if(hash == 5){
				std::string str_hash_dest = crc32(std::to_string(dest));
				unsigned int hash_dest = strtol(str_hash_dest.substr(0,8).c_str(), NULL, 16);
				std::string str_hash_src = crc32(std::to_string(src));
				unsigned int hash_src = strtol(str_hash_src.substr(0,8).c_str(), NULL, 16);
				seed = hash_dest + hash_src;
			}

			if(hash > 0)
				random_engine.seed(seed);

			for(int i=0; i < num_sample; i++) {
				float rnd = uniform_dist(random_engine);
				n[(int)(rnd*1000000)]++ ;
			}
		}
	}
	gettimeofday(&end, 0);

	printf("sampling time: %f\n", TimeDiff(start, end));

	unsigned long acc = 0;
	for(int i=0; i<1000000; i++) {
		acc += n[i];
		fprintf(fp_output, "%.8f\n", acc / (float)(graph->GetNumEdges() * num_sample));
	}
}

int main(int argc, char** argv) {
	int kNumSample = atoi(argv[1]);
	Graph graph(argv[2]);
	int hash = atoi(argv[3]);
	FILE* fp = fopen(argv[4], "w+");

	make_random(&graph, kNumSample, fp, hash);
}