#include "graph_engine.hpp"
#include <iostream>
#include "bitmap.hpp"

const unsigned int kInitValue = std::numeric_limits<unsigned int>::max();

unsigned int limit_distance;

unsigned int* median_distance;

Bitmap stopped_vertex;
class KnnVertex: public Vertex{
	unsigned int distance;
	unsigned int stop;

public:
	void Init(){
		distance = kInitValue;
		stop = false;
	}

	void SetStop(bool stop){
		this->stop = stop;
	}

	bool GetStop(){
		return stop;
	}

	void Start(){
		distance = 0;
		stop = true;
	}

	unsigned int GetDistance(){
		return distance;
	}

	void SetDistance(unsigned int distance){
		this->distance = distance;
	}

	bool operator==(const KnnVertex& v) const{
		return v.distance == distance && v.stop == stop;
	}

	size_t operator()(const KnnVertex& v) const{
		size_t res = 0;
		memcpy(&res, &v.distance, sizeof(unsigned int));
		return res;
	}
};

class KnnProgram: public VertexProgram<KnnVertex> {
public:
	bool ScatterBySrc(KnnVertex& src, EdgeList& out_edge_list, EdgeList& in_edge_list) {
		if(GetGraphEngine()->GetCurrentIteration() == 1){
			if(src.GetStop()) {
				src.SetStop(false);
				return true;
			}
			return false;
		}
		return true;
	}
	bool GatherByDest(KnnVertex& src, KnnVertex& dest, Edge& edge, EdgeAttr edge_attr) {
		unsigned int distance = src.GetDistance()+edge_attr;
		if(distance < dest.GetDistance()) {
			dest.SetDistance(distance);
			if(distance > limit_distance) {
				dest.SetStop(true);
				if(!stopped_vertex.Get(GetCurrentDestId())) {
					stopped_vertex.Set(GetCurrentDestId());
//					printf("%d stop\n", GetCurrentDestId());
				}
				return false;
			}
			return true;
		}
		return false;
	}
};


class KnnQueryThread: public QueryThread<KnnVertex, std::vector<VertexId>>{
	VertexId current;
	std::vector<unsigned int> distance_array;
	std::vector<VertexId> res;


public:
	KnnQueryThread(unsigned int tid, GraphEngine<KnnVertex>* ptr_graph_engine, VertexAllocator* ptr_vertex_allocator)
			:QueryThread(tid, ptr_graph_engine, ptr_vertex_allocator){
		current = ~0;
	}

	std::vector<VertexId> GetResult(){
		if(this->GetTid() == 0) {
//			for (int i = 0; i < res.size(); i++)
//				printf("%d ", res[i]);
//			printf("\n");
		}
		return res;
	}

	virtual void Merge(QueryThread* query_thread){
		if(GetTid() != query_thread->GetTid()){
			std::vector<VertexId> tmp = ((KnnQueryThread*) query_thread)->GetResult();
			res.reserve(res.size()+tmp.size());
			res.insert(res.end(), tmp.begin(), tmp.end());
		}
	}

	virtual void Query(KnnVertex& vertex, size_t duplicated){
		VertexId vertex_id = GetCurrentVertexId();

		if(median_distance[vertex_id]==~0U) {
			unsigned int distance = vertex.GetDistance();
			if (current != vertex_id) {
				current = vertex_id;
				distance_array.clear();
				std::vector<unsigned int>(distance_array).swap(distance_array);
			}

			for (int i = 0; i < duplicated; i++) {
				//todo: duplicated 처리
				distance_array.push_back(distance);
			}
		}
	}

	virtual void GlobalQuery(VertexId vertex_id){
		if(distance_array.size()) {
			std::nth_element(distance_array.begin(), distance_array.begin() + distance_array.size() / 2,
			                 distance_array.end());
			unsigned int median = distance_array[distance_array.size() / 2];
			if (median < limit_distance) {
				median_distance[vertex_id] = median;
//				printf("%d %d\n",vertex_id,median);
				res.push_back(vertex_id);
			}
		}
	}
};

int main(int argc, char** argv){
	Config config(argc, argv);
	config.SetHasEdgeAttribute(true);
	GraphEngine<KnnVertex> graph_engine = GraphEngine<KnnVertex>(config);

	median_distance = (unsigned int*) malloc(sizeof(unsigned int)* (graph_engine.GetGraph()->GetMaxVertexId()+1));
	memset(median_distance, 0xff, sizeof(unsigned int)* (graph_engine.GetGraph()->GetMaxVertexId()+1));

	graph_engine.BatchSampling();

	const VertexId start_vertex = 1;
	unsigned long res = 0;

	unsigned int mode = config.GetSampleProcessMode();

	struct timeval start, end;
	gettimeofday(&start, 0);

	if(mode == 0 && NUM_SAMPLE > 1)
		logging::core::get()->set_filter(logging::trivial::severity >= logging::trivial::info);

	stopped_vertex.Init(graph_engine.GetGraph()->GetMaxVertexId()+1);

	unsigned int max = 1000;
	unsigned int num_current_repeated = 0;
	do{
		int n = 0;
		limit_distance=0;

		while(n<max) {
			limit_distance+=50;
			if(n == 0) {
				stopped_vertex.ClearAll();
				graph_engine.InitVertex();
				graph_engine.SetStartVertex(start_vertex);
			}else{
				int a = 0;
				for(int i=0; i<graph_engine.GetGraph()->GetMaxVertexId()+1; i++){
					if(stopped_vertex.Get(i)) {
						graph_engine.SetStartVertex(i);
                        graph_engine.call_start_func = false;
						a++;
					}
				}
				printf("%d started\n", a);
				stopped_vertex.ClearAll();
			}

			graph_engine.Run<KnnProgram>();
			std::vector<VertexId> res = graph_engine.Query<KnnQueryThread, std::vector<VertexId>>();
			n += res.size();
			printf("%d selected\n",n);
		}
		num_current_repeated++;

	}while(mode == 0 && num_current_repeated < NUM_SAMPLE);

	gettimeofday(&end, 0);

	std::cout << "total running time: " << TimeDiff(start, end) << "\n";

	std::cout << "result: " << res << "\n";
}
