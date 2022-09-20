#include "graph_engine.hpp"
#include <iostream>

class WccVertex: public Vertex{
	unsigned int component;

public:
	void Init(VertexId id){
		component = id;
	}

	void Start(){
	}

	unsigned int GetComponent(){
		return component;
	}

	void SetComponent(unsigned int component){
		this->component = component;
	}

	bool operator==(const WccVertex& v) const{
		return v.component == component;
	}

	size_t operator()(const WccVertex& v) const{
		return v.component;
	}
};

class WccProgram: public VertexProgram<WccVertex> {
public:
	bool GatherByDest(WccVertex& src, WccVertex& dest, Edge& edge, EdgeAttr edge_attr) {
		unsigned int component = src.GetComponent();
		if(component < dest.GetComponent()) {
			dest.SetComponent(component);
			return true;
		}
		return false;
	}
};

class WccQueryThread: public QueryThread<WccVertex, unsigned long>{
	unsigned long component_sum;
	unsigned int num;

public:
	WccQueryThread(unsigned int tid, GraphEngine<WccVertex>* ptr_graph_engine, VertexAllocator* ptr_vertex_allocator)
			:QueryThread(tid, ptr_graph_engine, ptr_vertex_allocator){
		component_sum = 0;
	}

	unsigned long GetResult(){
		return component_sum;
	}

	unsigned long GetComponentSum(){
		return component_sum;
	}

	virtual void Merge(QueryThread* query_thread){
		if(GetTid() != query_thread->GetTid()){
			component_sum += ((WccQueryThread*) query_thread)->GetComponentSum();
		}
	}

	virtual void Query(WccVertex& vertex, size_t duplicated){
		unsigned int component = vertex.GetComponent();
		component_sum += component * duplicated;
	}
};

int main(int argc, char** argv){
	Config config(argc, argv);
	GraphEngine<WccVertex> graph_engine = GraphEngine<WccVertex>(config);

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
		graph_engine.ActivateAll();
		graph_engine.Run<WccProgram>();
		res += graph_engine.Query<WccQueryThread, unsigned long>();
		num_current_repeated++;
	}while(mode == 0 && num_current_repeated < NUM_SAMPLE);

	gettimeofday(&end, 0);

	std::cout << "total running time: " << TimeDiff(start, end) << "\n";

	std::cout << "result: " << res << "\n";
}
