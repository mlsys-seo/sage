#ifndef SAGE_VERTEX_HPP
#define SAGE_VERTEX_HPP

#include "graph.hpp"
#include "bitmap.hpp"
#include <unordered_map>
#include <list>
#include <mutex>

#define INVALID_VERTEX_ID ~0x00

class Vertex {
public:
	void Start(){
		assert(0);
	}

	bool operator==(Vertex& v) const{
		assert(0);
		return 0;
	}

	size_t operator()(Vertex& v) const{
		assert(0);
		return 0;
	}
};
#endif //SAGE_VERTEX_HPP
