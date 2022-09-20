#include "graph_engine.hpp"
#include <iostream>
#include <random>

float DAMPING_FACTOR = 0.85;
float TOLERANCE = 1.0E-4;
unsigned int MAX_ITERATION = 10;

VertexId start_vertex = 1;
class PageRankVertex: public Vertex{
	float new_rank;
	float current_rank;
	float delta;
	unsigned int degree;

public:
	void Init(){
		current_rank = 0; //1 - DAMPING_FACTOR;
		new_rank = current_rank;
		delta = current_rank;
		degree = 0;
	}

	void Start(){
		current_rank = 1 - DAMPING_FACTOR;
		new_rank = current_rank;
		delta = current_rank;
	}

	unsigned int GetDegree(){
		return degree;
	}

	void AddDegree(unsigned int x){
		degree += x;
	}

	float GetDelta(){
		return delta;
	}

	void SetDelta(float delta){
		this->delta = delta;
	}

	float GetCurrentRank(){
		return current_rank;
	}

	void SetCurrentRank(float current_rank){
		this->current_rank = current_rank;
	}

	float GetNewRank(){
		return new_rank;
    }

    void AddNewRank(float new_rank){
		this->new_rank += new_rank;
	}

	bool operator==(const PageRankVertex& v) const{
//		return !memcmp(this, &v, sizeof(PageRankVertex));
		return v.new_rank == new_rank && v.current_rank == current_rank && v.delta == delta && v.degree == degree;
	}

	size_t operator()(const PageRankVertex& v) const{
		size_t res=0;
		memcpy(&res, &v, sizeof(size_t) > sizeof(PageRankVertex) ? sizeof(PageRankVertex) : sizeof(size_t));
		return res;
	}
};

class PageRankProgram: public VertexProgram<PageRankVertex> {
public:
	bool ScatterBySrc(PageRankVertex& src, EdgeList& out_edge_list, EdgeList& in_edge_list){
		unsigned int iteration = GetGraphEngine()->GetCurrentIteration();
		if(iteration > MAX_ITERATION) {
			return false;
		} else if(iteration == 1){
			return true;
		} else if(iteration == 2){
			if(GetCurrentSrcId() == start_vertex) {
				src.Start();
				src.SetDelta(src.GetCurrentRank() / src.GetDegree() * DAMPING_FACTOR);
				return true;
			}
			return false;
		} else if(src.GetNewRank() - src.GetCurrentRank() > TOLERANCE){
			src.SetDelta((src.GetNewRank() - src.GetCurrentRank()) / src.GetDegree() * DAMPING_FACTOR);
			src.SetCurrentRank(src.GetNewRank());
			return true;
		}
		return false;
	}

    bool GatherByDest(PageRankVertex& src, PageRankVertex& dest, Edge& edge, EdgeAttr edge_attr) {
		if(GetGraphEngine()->GetCurrentIteration() == 1)
			dest.AddDegree(1);
		else
		    dest.AddNewRank(src.GetDelta());
	    return true;
    }
};

class PageRankQueryThread: public QueryThread<PageRankVertex,float>{
	float rank_sum;
	unsigned int num;

public:
	PageRankQueryThread(unsigned int tid, GraphEngine<PageRankVertex>* ptr_graph_engine, VertexAllocator* ptr_vertex_allocator)
			:QueryThread(tid, ptr_graph_engine, ptr_vertex_allocator){
		rank_sum = 0;
	}

	float GetResult(){
		return rank_sum;
	}

	float GetRankSum(){
		return rank_sum;
	}

	void Merge(QueryThread* query_thread){
		if(GetTid() != query_thread->GetTid()){
			rank_sum += ((PageRankQueryThread*) query_thread)->GetRankSum();
		}
	}

	void Query(PageRankVertex& vertex, size_t duplicated){
		rank_sum += vertex.GetCurrentRank() * duplicated;
	}
};

int main(int argc, char** argv){
	Config config(argc, argv);
	GraphEngine<PageRankVertex> graph_engine = GraphEngine<PageRankVertex>(config);

	graph_engine.BatchSampling();

	float res = 0;

	unsigned int mode = config.GetSampleProcessMode();

	struct timeval start, end;
	gettimeofday(&start, 0);

	if(mode == 0 && NUM_SAMPLE > 1)
		logging::core::get()->set_filter(logging::trivial::severity >= logging::trivial::info);


	unsigned int num_current_repeated = 0;
	do{
		graph_engine.ActivateAll();
		graph_engine.InitVertex();
		graph_engine.Run<PageRankProgram>();
		res += graph_engine.Query<PageRankQueryThread, float>();
		num_current_repeated++;
	}while(mode == 0 && num_current_repeated < NUM_SAMPLE);

	gettimeofday(&end, 0);

	std::cout << "total running time: " << TimeDiff(start, end) << "\n";

	std::cout << "result: " << res << "\n";
}
