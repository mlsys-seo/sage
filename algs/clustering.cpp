#include "graph_engine.hpp"
#include <iostream>

const bool kInitValue = false;
int STAGE = 0;
const int NUM_CLUSTER = 4;
unsigned int* reliability[NUM_CLUSTER];
unsigned int* cluster;

class ClusterVertex: public Vertex {
	bool visited;

public:
	void Init() {
		visited = kInitValue;
	}

	void Start() {
		visited = true;
	}

	bool GetVisited() const {
		return visited;
	}

	void SetVisited() {
		this->visited = true;
	}

	bool operator==(const ClusterVertex& v) const{
		return v.visited == visited;
	}
};

class ClusterProgram: public VertexProgram<ClusterVertex> {
public:
	bool GatherByDest(ClusterVertex& src, ClusterVertex& dest, Edge& edge, EdgeAttr edge_attr){
		if(STAGE == 2) {
			if (cluster[GetCurrentDestId()] != 0)
				return false;
		}else if(STAGE == 3){
			if (cluster[GetCurrentDestId()] != 1)
				return false;
		}

		if(dest.GetVisited() == kInitValue) {
			dest.SetVisited();
			return true;
		}
		return false;
	}
};

class ClusterQueryThread: public QueryThread<ClusterVertex, VertexId>{
	VertexId min_vertex;
	unsigned int min_vertex_degree;
	unsigned int min_cnt;

public:
	ClusterQueryThread(unsigned int tid, GraphEngine<ClusterVertex>* ptr_graph_engine, VertexAllocator* ptr_vertex_allocator)
			:QueryThread(tid, ptr_graph_engine, ptr_vertex_allocator){
		min_vertex = ~0U;
		min_cnt = ~0;
		min_vertex_degree = 0;
	}

	VertexId GetResult(){
		return min_vertex;
	}

	std::pair<VertexId, unsigned int> GetMin(){
		return {min_vertex, min_cnt};
	}

	void Merge(QueryThread* query_thread){
		if(GetTid() != query_thread->GetTid()){
			auto tmp = ((ClusterQueryThread *) query_thread)->GetMin();
			if (tmp.first == ~0U)
				return;
			if (min_cnt > tmp.second) {
				min_cnt = tmp.second;
				min_vertex = tmp.first;
			}else if(min_cnt == tmp.second){
				unsigned int degree = GetPtrGraph()->GetOutEdgeList(tmp.first).GetDegree();
				if(degree > min_vertex_degree) {
					min_vertex = tmp.first;
					min_vertex_degree = degree;
				}
				else if(min_vertex_degree == degree)
					min_vertex = min_vertex > tmp.first ? min_vertex : tmp.first;
			}
		}
	}

	void Query(ClusterVertex& vertex, size_t duplicated){
		VertexId vertex_id = GetCurrentVertexId();
		if( vertex.GetVisited() ) {
			reliability[STAGE][vertex_id]+= duplicated;
		}
	}

	void GlobalQuery(VertexId vertex_id){
		if(STAGE < 2) {
			if (reliability[STAGE][vertex_id] == 0)
				return;
		}

		if(STAGE == 0) {
			if(min_cnt > reliability[0][vertex_id]) {
				min_cnt = reliability[0][vertex_id];
				min_vertex = vertex_id;
				min_vertex_degree = GetPtrGraph()->GetOutEdgeList(vertex_id).GetDegree();
			}else if (min_cnt == reliability[0][vertex_id]) {
				unsigned int degree = GetPtrGraph()->GetOutEdgeList(vertex_id).GetDegree();
				if (degree > min_vertex_degree) {
					min_vertex = vertex_id;
					min_vertex_degree = degree;
				} else if (min_vertex_degree == degree)
					min_vertex = min_vertex > vertex_id ? min_vertex : vertex_id;
			}
		}else if(STAGE == 1){
			assert(cluster[vertex_id] == 0);
			if(reliability[0][vertex_id] > reliability[1][vertex_id]) {
				cluster[vertex_id] = 0;
			} else {
				cluster[vertex_id] = 1;
			}

			if(cluster[vertex_id] == 0) {
				if (min_cnt > reliability[0][vertex_id]) {
					min_cnt = reliability[0][vertex_id];
					min_vertex = vertex_id;
					min_vertex_degree = GetPtrGraph()->GetOutEdgeList(vertex_id).GetDegree();
				}else if (min_cnt == reliability[0][vertex_id]) {
					unsigned int degree = GetPtrGraph()->GetOutEdgeList(vertex_id).GetDegree();
					if(degree > min_vertex_degree) {
						min_vertex = vertex_id;
						min_vertex_degree = degree;
					}
					else if(min_vertex_degree == degree)
						min_vertex = min_vertex > vertex_id ? min_vertex : vertex_id;
				}
			}
		}else if(STAGE == 2){
			if(cluster[vertex_id] == 0) {
				if (reliability[0][vertex_id] >= reliability[2][vertex_id]) {
					cluster[vertex_id] = 0;
				} else {
					cluster[vertex_id] = 2;
				}
			}
			if(cluster[vertex_id] == 1) {
				if (min_cnt > reliability[1][vertex_id]) {
					min_cnt = reliability[1][vertex_id];
					min_vertex = vertex_id;
				}else if (min_cnt == reliability[1][vertex_id]) {
					min_vertex = min_vertex > vertex_id ? min_vertex : vertex_id;
				}
			}
		}else if(STAGE == 3){
			if(cluster[vertex_id] == 1) {
				if (reliability[1][vertex_id] > reliability[3][vertex_id]) {
					cluster[vertex_id] = 1;
				} else {
					cluster[vertex_id] = 3;
				}
			}
		}
	}
};

int main(int argc, char** argv){
	Config config(argc, argv);
	GraphEngine<ClusterVertex> graph_engine = GraphEngine<ClusterVertex>(config);

	graph_engine.BatchSampling();

	VertexId start_vertex = 1;
	std::pair<unsigned int, VertexId> res = {0, 0};

	struct timeval start, end;
	gettimeofday(&start, 0);

	for(int i=0; i<NUM_CLUSTER; i++) {
		reliability[i] = new unsigned int[graph_engine.GetGraph()->GetMaxVertexId() + 1]();
		cluster = new unsigned int[graph_engine.GetGraph()->GetMaxVertexId() + 1]();
		for (int j = 0; j < graph_engine.GetGraph()->GetMaxVertexId() + 1; j++)
			reliability[i][j] = 0;
	}

	for(int i=0; i<NUM_CLUSTER; i++) {
		printf("%d start\n", start_vertex);
		graph_engine.InitVertex();
		if(start_vertex != ~0) {
			graph_engine.SetStartVertex(start_vertex);
			graph_engine.Run<ClusterProgram>();
		}
		start_vertex = graph_engine.Query<ClusterQueryThread, VertexId>();
		STAGE = i+1;
	}

	gettimeofday(&end, 0);

	std::cout << "total running time: " << TimeDiff(start, end) << "\n";

	std::cout << "result: " << start_vertex << "\n";
}
