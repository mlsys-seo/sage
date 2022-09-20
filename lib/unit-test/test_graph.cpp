#include "graph.hpp"

int main(int argc, char** argv) {
//	Graph g2;
//	g2.LoadText(argv[1]);
//	g2.Save(argv[2],argv[3]);

	Graph g;
	g.Load(argv[2], argv[3]);

#if 1
	printf("index: \n");
//	g.PrintIndex(false);

//	printf("\n\n");
//	printf("graph: \n");
	g.PrintEdgeList(false);
#else
	printf("type %d\n", g.GetType());
	printf("max vid %d\n", g.GetMaxVertexId());
	printf("num edges %d\n", g.GetType());
#endif

	return 0;
};
