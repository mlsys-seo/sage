#ifndef SAGE_SAMPLING_THREAD_HPP
#define SAGE_SAMPLING_THREAD_HPP

#include <unordered_map>
#include <random>

#include "allocator.hpp"
#include "graph.hpp"
#include "config.hpp"

typedef unsigned int (*HashFuncPtr) (unsigned int, unsigned int) ;

class SamplingThread: public Thread {
	unsigned int tid;
	unsigned int part_size;
	VertexAllocator* ptr_vertex_allocator;
	Graph* ptr_graph;
	Bit64** sampling_index;
	size_t num_sample;

public:
	SamplingThread()
		:Thread(tid){

	}

	SamplingThread(unsigned int tid, VertexAllocator* ptr_vertex_allocator, Graph* ptr_graph, Bit64** sampling_index, size_t num_sample, unsigned int part_size)
			: Thread(tid)
			, tid(tid)
			, ptr_graph(ptr_graph)
			, sampling_index(sampling_index)
			, num_sample(num_sample)
			, part_size(part_size)
			, ptr_vertex_allocator(ptr_vertex_allocator) {
	}

	static void Sampling(Bit64* ptr_data, VertexId src, VertexId dest,  unsigned int num_sample, float edge_prob, HashFuncPtr hash_func_ptr = NULL){
		const unsigned int kPrime = 10000007;

		if(edge_prob == 0){
			BaseBitmap::ClearAll(ptr_data, NUM_SAMPLE);
			return;
		}
		if(edge_prob == 1.0){
			BaseBitmap::SetAll(ptr_data, NUM_SAMPLE);
			return;
		}

		std::default_random_engine random_engine(0);
		//std::uniform_real_distribution<float> uniform_dist(0.0, 1.0);
		std::uniform_int_distribution<unsigned int> uniform_dist(0, std::numeric_limits<unsigned int>::max());

		unsigned int seed;
		if(hash_func_ptr == NULL) {
			seed = dest * kPrime + src + SEED;
		} else {
			seed = hash_func_ptr(src, dest);
		}

		random_engine.seed(seed);
#if 1
		unsigned int _edge_prob = edge_prob * 256U;
		for(unsigned int i=0; i < num_sample; i+=4){
			unsigned int _rnd = uniform_dist(random_engine);
			unsigned int rnd0 = _rnd & 0xff;
			unsigned int rnd1 = (_rnd & 0xff00) >> 16;
			unsigned int rnd2 = (_rnd & 0xff0000) >> 32;
			unsigned int rnd3 = (_rnd & 0xff000000) >> 48;

			if(rnd0 <= _edge_prob)
				BaseBitmap::Set(ptr_data, i);
			if(rnd1 <= _edge_prob)
				BaseBitmap::Set(ptr_data, i+1);
			if(rnd2 <= _edge_prob)
				BaseBitmap::Set(ptr_data, i+2);
			if(rnd3 <= _edge_prob)
				BaseBitmap::Set(ptr_data, i+3);
		}
#else

		for(unsigned int i=0; i < num_sample; i++){
			float rnd = uniform_dist(random_engine);
			if(rnd <= edge_prob)
				BaseBitmap::Set(ptr_data, i);
		}
#endif
	}

	void Run() {
		std::vector<VertexId> allocated_vertices;
		do {
			allocated_vertices = ptr_vertex_allocator->GetVertices(part_size);
			for (int i = 0; i < allocated_vertices.size(); i++) {
				VertexId dest = allocated_vertices[i];
				EdgeList in_edge_list = ptr_graph->GetInEdgeList(dest);

				for (int j = 0; j < in_edge_list.GetDegree(); j++) {
					Edge &edge = in_edge_list.GetEdge(j);
					VertexId src = edge.neighbor;
					Bitmap* sampling_bitmap = new Bitmap(num_sample);
					Sampling(sampling_bitmap->GetData(), src, dest, num_sample, edge.probability);
					off_t edge_off = (&edge) - ptr_graph->GetInEdges();
					sampling_index[edge_off] = sampling_bitmap->GetData();
				}
			}
		} while (!allocated_vertices.empty());
	}
};


#endif //SAGE_SAMPLING_THREAD_HPP
