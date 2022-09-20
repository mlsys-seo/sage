#include "graph_engine.hpp"
#include <iostream>
#include "bitmap.hpp"

const int kInitValue = 0x00;
const int kNumStart = 64;
unsigned int start_idx=0;

class InfluenceMaxVertex: public Vertex {
	uint64_t visited;
public:

	void Init() {
		visited = 0UL;
	}

	void Start() {
		visited |= (0x01UL << start_idx);
	}

	uint64_t GetVisited(){
		return visited;
	}

	void Or(uint64_t visited){
		this->visited |=  visited;
	}

	bool operator==(const InfluenceMaxVertex& v) const{
		return v.visited == visited;
	}
};

class InfluenceMaxProgram: public VertexProgram<InfluenceMaxVertex> {
public:
	bool GatherByDest(InfluenceMaxVertex& src, InfluenceMaxVertex& dest, Edge& edge, EdgeAttr edge_attr){
		uint64_t src_bitmap = src.GetVisited();
		uint64_t dest_bitmap = dest.GetVisited();
		bool changed = true;
		dest.Or(src.GetVisited());
		uint64_t tmp = dest.GetVisited();
		if(tmp == dest_bitmap)
			changed = false;
		return changed;
	}
};

class InfluenceMaxQueryThread: public QueryThread<InfluenceMaxVertex, unsigned int>{
	unsigned int num[kNumStart];

public:
	InfluenceMaxQueryThread(unsigned int tid, GraphEngine<InfluenceMaxVertex>* ptr_graph_engine, VertexAllocator* ptr_vertex_allocator)
			:QueryThread(tid, ptr_graph_engine, ptr_vertex_allocator){
		memset(num, 0x00, sizeof(unsigned int) * kNumStart);

	}

	unsigned int GetResult(){
		unsigned int inf_max_idx = 0;
		unsigned int max_num = num[0];
		for(int i=1; i<kNumStart; i++) {
			if(num[i] > max_num){
				max_num = num[i];
				inf_max_idx = i;
			}
		}
		return inf_max_idx;
	}

	unsigned int* GetNum(){
		return num;
	}

	void Merge(QueryThread* query_thread){
		if(GetTid() != query_thread->GetTid()){
			for(int i=0; i<kNumStart; i++)
				num[i] += ((InfluenceMaxQueryThread*) query_thread)->GetNum()[i];
		}
	}

	void Query(InfluenceMaxVertex& vertex, size_t duplicated){
		uint64_t visited = vertex.GetVisited();

		if(visited){
			for(int i=0; i<kNumStart; i++) {
				if(visited & (0x01UL << i)){
					num[i] += duplicated;
				}
			}
		}
	}
};

int main(int argc, char** argv){
	Config config(argc, argv);
	GraphEngine<InfluenceMaxVertex> graph_engine(config);

	graph_engine.BatchSampling();

	struct timeval start, end;
	gettimeofday(&start, 0);

	graph_engine.InitVertex();
	std::vector<VertexId> start_vertex;
	srand(0);
	for(int i=0; i<kNumStart; i++) {
		VertexId r = rand() % (graph_engine.GetGraph()->GetMaxVertexId() + 1);
		start_vertex.push_back(r);
	}

	start_idx = 0;
	for(int i=0; i<kNumStart; i++) {
		graph_engine.SetStartVertex(start_vertex[i]);
		start_idx++;
	}

	graph_engine.Run<InfluenceMaxProgram>();
	unsigned int res = graph_engine.Query<InfluenceMaxQueryThread, unsigned int>();

	gettimeofday(&end, 0);

	std::cout << "total running time: " << TimeDiff(start, end) << "\n";

	std::cout << "result: " << start_vertex[res] << "\n";
}
