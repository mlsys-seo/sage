#include "graph_engine.hpp"
#include <iostream>
#include <random>
enum{
	INIT_DEGREE,
	KCORE
};
unsigned int core_degree = 0;
int stage = INIT_DEGREE;
unsigned int* gvertex;
int limit = 5;
class KcoreVertex: public Vertex{

public:
	unsigned int deleted;
	unsigned int degree;

	void Init(){
		degree = 0;
		deleted = false;
	}

	void Start(){

	}

	bool operator==(const KcoreVertex& v) const{
		return deleted == v.deleted && degree == v.degree;
	}
};

class KcoreProgram: public VertexProgram<KcoreVertex> {
public:
	bool ScatterBySrc(KcoreVertex& src, EdgeList& out_edge_list, EdgeList& in_edge_list){
		unsigned int iter = GetGraphEngine()->GetCurrentIteration();
#if 0
		if(iter>limit)
			return false;

		if(src.deleted)
			return false;
		if(src.degree < iter - 1)
			return false;

		src.deleted = true;
		src.degree = 0;
		return true;
#else
		if(iter>limit)
			return false;
		if(iter == 1)
			return true;
		if(src.degree < iter && src.deleted == false) {
			src.deleted = true;
			return true;
		}
		return false;
#endif
	}

	bool GatherByDest(KcoreVertex& src, KcoreVertex& dest, Edge& edge, EdgeAttr edge_attr) {
		unsigned int iter = GetGraphEngine()->GetCurrentIteration();
#if 0
		dest.degree += 1;
		if(dest.degree > iter)
			dest.degree = iter;
		dest.deleted = false;
		return true;
#else
		if(iter == 1) {
			dest.degree++;
		}else{
			assert(dest.degree > 0);
			dest.degree--;
		}
		return true;
#endif
	}
};

class KcoreQueryThread: public QueryThread<KcoreVertex,float>{

public:
	KcoreQueryThread(unsigned int tid, GraphEngine<KcoreVertex>* ptr_graph_engine, VertexAllocator* ptr_vertex_allocator)
			:QueryThread(tid, ptr_graph_engine, ptr_vertex_allocator){
	}

	float GetResult(){
		return 0;
	}

	void Merge(QueryThread* query_thread){
		if(GetTid() != query_thread->GetTid()){
		}
	}

	void Query(KcoreVertex& vertex, size_t duplicated){
		VertexId vertex_id = GetCurrentVertexId();

		if( !vertex.deleted ) {
//			assert(vertex.degree >= limit);
			gvertex[vertex_id] += duplicated;
		}
	}

	virtual void GlobalQuery(VertexId vertex_id){
/*
		if(gvertex[vertex_id] / (float)NUM_SAMPLE >= limit - 1)
			printf("core : %d - %d\n",vertex_id, gvertex[vertex_id]);
			*/
	}
};

int main(int argc, char** argv){
	Config config(argc, argv);
	GraphEngine<KcoreVertex> graph_engine = GraphEngine<KcoreVertex>(config);

	graph_engine.BatchSampling();

	float res = 0;

	unsigned int mode = config.GetSampleProcessMode();

	struct timeval start, end;
	gettimeofday(&start, 0);

	gvertex = new unsigned int[graph_engine.GetGraph()->GetMaxVertexId()+1]();
	if(mode == 0 && NUM_SAMPLE > 1)
		logging::core::get()->set_filter(logging::trivial::severity >= logging::trivial::info);

	unsigned int num_current_repeated = 0;
	do{
		graph_engine.ActivateAll();
		graph_engine.InitVertex();
		graph_engine.Run<KcoreProgram>();
		res += graph_engine.Query<KcoreQueryThread, float>();
		num_current_repeated++;
	}while(mode == 0 && num_current_repeated < NUM_SAMPLE);

	gettimeofday(&end, 0);

	std::cout << "total running time: " << TimeDiff(start, end) << "\n";

	std::cout << "result: " << res << "\n";
}
