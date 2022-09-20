#include "graph_engine.hpp"
#include <iostream>

const bool kInitValue = false;

unsigned int* gvertex;

class TopkVertex: public Vertex {
	bool visited;

public:
	void Init() {
		visited = kInitValue;
	}

	void Start() {
		visited = kInitValue;
	}

	bool GetVisited() const {
		return visited;
	}

	void SetVisited() {
		this->visited = true;
	}

	bool operator==(const TopkVertex& v) const{
		return v.visited == visited;
	}
};

class TopkProgram: public VertexProgram<TopkVertex> {
public:
//	bool ScatterBySrc(TopkVertex& src, EdgeList& out_edge_list, EdgeList& in_edge_list){
//		if(src.GetVisited() == kInitValue) {
//			src.SetVisited();
//			return true;
//		}
//		return false;
//	}
	bool GatherByDest(TopkVertex& src, TopkVertex& dest, Edge& edge, EdgeAttr edge_attr){
//		return true;
		if(dest.GetVisited() == kInitValue) {
			dest.SetVisited();
			return true;
		}
		return false;
	}
};

const int k = 100;

class TopkQueryThread: public QueryThread<TopkVertex, float>{
	std::vector<std::pair<unsigned int, VertexId>> topk_vertex;

public:
	TopkQueryThread(unsigned int tid, GraphEngine<TopkVertex>* ptr_graph_engine, VertexAllocator* ptr_vertex_allocator)
			:QueryThread(tid, ptr_graph_engine, ptr_vertex_allocator){
	}

	float GetResult(){
		std::vector<std::pair<float, VertexId>> res_vec;
		for(int i=0; i<topk_vertex.size(); i++){
			res_vec.push_back({topk_vertex[i].first/(float)NUM_SAMPLE, topk_vertex[i].second});
		}

		float res = 0;
		for(int i=0; i<res_vec.size(); i++) {
			res += res_vec[i].first;
		}
		return res / k;
	}

	std::vector<std::pair<unsigned int, VertexId>> GetTopkVertex(){
		return topk_vertex;
	}

	void Merge(QueryThread* query_thread){
		if(GetTid() != query_thread->GetTid()){
			auto tmp = ((TopkQueryThread*) query_thread)->GetTopkVertex();
			for(int i=0; i<tmp.size(); i++) {
				if(topk_vertex.size() > k-1) {
					auto min_it = topk_vertex.begin();
					unsigned int min = ~0U;

					for (auto it = topk_vertex.begin(); it != topk_vertex.end(); it++) {
						if (min > it->first) {
							min = it->first;
							min_it = it;
						}
					}

					if (tmp[i].first > min) {
						min_it->first = tmp[i].first;
						min_it->second = tmp[i].second;
					}
				}else{
					topk_vertex.push_back({tmp[i].first, tmp[i].second});
				}
			}
		}
	}

	void Query(TopkVertex& vertex, size_t duplicated){
		VertexId vertex_id = GetCurrentVertexId();

		if( vertex.GetVisited() ) {
			gvertex[vertex_id] += duplicated;
		}
	}

	void GlobalQuery(VertexId vertex_id){
		if(topk_vertex.size() > k-1) {
			auto min_it = topk_vertex.begin();
			unsigned int min = min_it->first;

			for(auto it = topk_vertex.begin(); it != topk_vertex.end(); it++){
				if (min > it->first) {
					min = it->first;
					min_it = it;
				}
			}

			if(gvertex[vertex_id] > min){
				min_it->first = gvertex[vertex_id];
				min_it->second = vertex_id;
			}
		}else{
			topk_vertex.push_back({gvertex[vertex_id], vertex_id});
		}
	}
};

int main(int argc, char** argv){
	Config config(argc, argv);
	GraphEngine<TopkVertex> graph_engine = GraphEngine<TopkVertex>(config);

	graph_engine.BatchSampling();

	const VertexId start_vertex = 1;
	float res = 0;

	unsigned int mode = config.GetSampleProcessMode();

	struct timeval start, end;
	gettimeofday(&start, 0);

	gvertex = new unsigned int[graph_engine.GetGraph()->GetMaxVertexId()+1]();

	if(mode == 0 && NUM_SAMPLE > 1)
		logging::core::get()->set_filter(logging::trivial::severity >= logging::trivial::info);

	unsigned int num_current_repeated = 0;
	do{
		graph_engine.InitVertex();
		graph_engine.SetStartVertex(start_vertex);
		graph_engine.Run<TopkProgram>();
		res += graph_engine.Query<TopkQueryThread, float>();
		num_current_repeated++;
	}while(mode == 0 && num_current_repeated < NUM_SAMPLE);

	gettimeofday(&end, 0);

	std::cout << "total running time: " << TimeDiff(start, end) << "\n";

	std::cout << "result: " << res << "\n";
}
