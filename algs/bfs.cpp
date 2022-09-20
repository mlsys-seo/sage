#include "graph_engine.hpp"
#include <iostream>

const int kInitValue = 0xff;

class BfsVertex: public Vertex {
	unsigned char hop;

public:
	void Init() {
		hop = kInitValue;
	}

	void Start() {
		hop = kInitValue;
	}

	unsigned char GetHop() const {
		return hop;
	}

	void SetHop(unsigned char hop) {
		this->hop = hop;
	}

	bool operator==(const BfsVertex& v) const{
		return v.hop == hop;
	}
};

class BfsProgram: public VertexProgram<BfsVertex> {
public:
//	bool ScatterBySrc(BfsVertex& src, EdgeList& out_edge_list, EdgeList& in_edge_list){
//		if(src.GetHop() == kInitValue) {
//			unsigned char hop = (unsigned char) GetGraphEngine()->GetCurrentIteration();
//			src.SetHop(hop);
//			return true;
//		}
//		return false;
//	}
	bool GatherByDest(BfsVertex& src, BfsVertex& dest, Edge& edge, EdgeAttr edge_attr){
#if 0
		return true;
#else
		if(dest.GetHop() == kInitValue) {
			unsigned char hop = (unsigned char) GetGraphEngine()->GetCurrentIteration();
			dest.SetHop(hop);
			return true;
		}
		return false;
#endif
	}
};

class BfsQueryThread: public QueryThread<BfsVertex, float>{
	unsigned int num;
	unsigned int sum;

public:
	BfsQueryThread(unsigned int tid, GraphEngine<BfsVertex>* ptr_graph_engine, VertexAllocator* ptr_vertex_allocator)
			:QueryThread(tid, ptr_graph_engine, ptr_vertex_allocator){
		num = 0;
		sum = 0;
	}

	float GetResult(){
		return sum;
	}

	unsigned int GetNum(){
		return num;
	}

	unsigned int GetSum(){
		return sum;
	}

	void Merge(QueryThread* query_thread){
		if(GetTid() != query_thread->GetTid()){
			sum += ((BfsQueryThread*) query_thread)->GetSum();
			num += ((BfsQueryThread*) query_thread)->GetNum();
		}
	}

	void Query(BfsVertex& vertex, size_t duplicated){
		unsigned char hop = vertex.GetHop();

		if(hop != kInitValue) {
			num += duplicated;
			sum += hop * duplicated;
		}
	}
};

int main(int argc, char** argv){
	Config config(argc, argv);
	GraphEngine<BfsVertex> graph_engine = GraphEngine<BfsVertex>(config);

	graph_engine.BatchSampling();

	const VertexId start_vertex = 1;
	float res = 0;

	unsigned int mode = config.GetSampleProcessMode();

	struct timeval start, end;
	gettimeofday(&start, 0);

	if(mode == 0 && NUM_SAMPLE > 1)
		logging::core::get()->set_filter(logging::trivial::severity >= logging::trivial::info);

	unsigned int num_current_repeated = 0;
	do{
		graph_engine.InitVertex();
		graph_engine.SetStartVertex(start_vertex);
		graph_engine.Run<BfsProgram>();
		res += graph_engine.Query<BfsQueryThread, float>();
		num_current_repeated++;
	}while(mode == 0 && num_current_repeated < NUM_SAMPLE);

	gettimeofday(&end, 0);

	std::cout << "total running time: " << TimeDiff(start, end) << "\n";

	std::cout << "result: " << res << "\n";
}
