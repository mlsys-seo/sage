#include "graph_engine.hpp"
#include <iostream>
const unsigned int kInitValue = std::numeric_limits<unsigned int>::max();

class ShortestPathVertex: public Vertex{
	unsigned int distance;

public:
	void Init() {
		distance = kInitValue;;
	}

	void Start() {
		distance = 0;
	}

	unsigned int GetDistance() {
		return distance;
	}

	void SetDistance(unsigned int distance) {
		this->distance = distance;
	}

	bool operator==(const ShortestPathVertex& v) const{
		return v.distance == distance;
	}

	size_t operator()(const ShortestPathVertex& v) const{
		size_t res = 0;
		memcpy(&res, &v.distance, sizeof(unsigned int));
		return res;
	}
};

class ShortestPathProgram: public VertexProgram<ShortestPathVertex> {
public:
	bool GatherByDest(ShortestPathVertex& src, ShortestPathVertex& dest, Edge& edge, EdgeAttr edge_attr) {
		unsigned int distance = src.GetDistance() + edge_attr;
		if(distance < dest.GetDistance()) {
			dest.SetDistance(distance);
			return true;
		}
		return false;
	}
};


class ShortestPathQueryThread: public QueryThread<ShortestPathVertex,unsigned long>{
	unsigned long distance_sum;
	unsigned int num;

public:
	ShortestPathQueryThread(unsigned int tid, GraphEngine<ShortestPathVertex>* ptr_graph_engine, VertexAllocator* ptr_vertex_allocator)
			:QueryThread(tid, ptr_graph_engine, ptr_vertex_allocator){
		distance_sum = 0;
		num = 0;
	}

	unsigned long GetResult(){
		return distance_sum;
	}

	unsigned int GetDistanceSum(){
		return distance_sum;
	}

	unsigned int GetNum(){
		return num;
	}

	virtual void Merge(QueryThread* query_thread){
		if(GetTid() != query_thread->GetTid()){
			distance_sum += ((ShortestPathQueryThread*) query_thread)->GetDistanceSum();
			num +=  ((ShortestPathQueryThread*) query_thread)->GetNum();
		}
	}

	virtual void Query(ShortestPathVertex& vertex, size_t duplicated){
		unsigned int distance = vertex.GetDistance();
		if(distance != kInitValue){
			distance_sum += distance * duplicated;
			num += duplicated;
		}
	}
};

int main(int argc, char** argv){
	Config config(argc, argv);
	config.SetHasEdgeAttribute(true);
	GraphEngine<ShortestPathVertex> graph_engine = GraphEngine<ShortestPathVertex>(config);

	graph_engine.BatchSampling();

	const VertexId start_vertex = 1;
	unsigned long res = 0;

	unsigned int mode = config.GetSampleProcessMode();

	struct timeval start, end;
	gettimeofday(&start, 0);

	if(mode == 0 && NUM_SAMPLE > 1)
		logging::core::get()->set_filter(logging::trivial::severity >= logging::trivial::info);

	unsigned int num_current_repeated = 0;
	do{
		graph_engine.InitVertex();
		graph_engine.SetStartVertex(start_vertex);
		graph_engine.Run<ShortestPathProgram>();
		res += graph_engine.Query<ShortestPathQueryThread, unsigned long>();
		num_current_repeated++;
	}while(mode == 0 && num_current_repeated < NUM_SAMPLE);

	gettimeofday(&end, 0);

	std::cout << "total running time: " << TimeDiff(start, end) << "\n";

	std::cout << "result: " << res << "\n";
}
