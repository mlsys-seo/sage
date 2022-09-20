#include "vertex.hpp"
#include <iostream>

class BfsVertex: public Vertex{
	unsigned char hop;

public:
	BfsVertex()
		: hop(0) {
		InitValue();
	}

	unsigned char GetHop(){
		return hop;
	}

	void InitValue(){
		memset(this, 0xff, sizeof(*this));
	}
};


int main(){
	const int kNumVertex = 1000;

	BfsVertex* v = new BfsVertex[kNumVertex];
	BfsVertex::SetVertexArray((void*) v, sizeof(BfsVertex));

	for(int i=0; i<kNumVertex; i++) {
		std::cout << "vertex: " << v[i].GetVertexId() << ", hop: " << (unsigned int) v[i].GetHop() << "\n";
	}
}