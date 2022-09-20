#include <random>
#include <iostream>
#include <sys/time.h>
#include "graph.hpp"
#include "timer.hpp"
#include <algorithm>

std::random_device rd;
std::default_random_engine random_engine(0);
std::uniform_real_distribution<float> uniform_dist(0.0, 1.0);

float sampling_time = 0;
bool directed = 0;
void make_random(Graph* graph, FILE* fp_output){
	const unsigned int kPrime = 100000007;

	struct timeval start, end;
	gettimeofday(&start, 0);

	for(unsigned int i=0; i<graph->GetMaxVertexId()+1; i++) {
		EdgeList edge_list = graph->GetOutEdgeList(i);
		for(unsigned int j=0; j<edge_list.GetDegree(); j++) {

			Edge& edge = edge_list.GetEdge(j);
			VertexId src = i;
			VertexId dest = edge.neighbor;

			if(directed == false && src > dest)
				continue;
//			EdgeAttr* edge_attrs = graph->GetAllEdgeAttrs();
//			off_t edge_off = &edge - graph->GetOutEdges();
//			float edge_attr = edge_attrs[edge_off];
			float rnd = uniform_dist(random_engine);
			float prob = edge.probability;
			if(prob > rnd) {
				if (directed == true){
					fprintf(fp_output, "%d %d\n", src, dest);
				}else{
					fprintf(fp_output, "%d %d\n", src, dest);
					fprintf(fp_output, "%d %d\n", dest, src);
				}

//				fprintf(fp_output, "%d %d %f\n", src, dest, edge_attr);
			}
		}
	}

	gettimeofday(&end, 0);
	sampling_time += TimeDiff(start, end);
}

int main(int argc, char** argv) {
	Graph graph(argv[1]);
	int seed = atoi(argv[2]);
	int num = atoi(argv[3]);
	directed = atoi(argv[4]);
	std::string output_file = argv[5];
	random_engine.seed(seed);
	for(int i=0; i<num; i++) {
		FILE *fp = fopen((output_file+"."+std::to_string(seed)).c_str(), "w+");
		make_random(&graph, fp);
		seed++;
	}
	printf("sampling time: %f\n", sampling_time);

}
