#ifndef SAGE_EDGE_MANAGER_HPP
#define SAGE_EDGE_MANAGER_HPP

#include <unordered_map>
#include <list>
#include <iostream>
#include <cstring>
#include <cassert>
#include "timer.hpp"
#include "config.hpp"

typedef unsigned int VertexId;
typedef float EdgeAttr;
//todo: change EdgeAttr to template

typedef struct{
	VertexId neighbor;
	float probability;
	VertexId next_dest;
} Edge;

class EdgeList{
	Edge* edges;
	off_t offset;
	size_t degree;

public:
	EdgeList(Edge* edges, off_t offset, size_t degree)
		:edges(edges)
		,offset(offset)
		,degree(degree){
	}

	inline off_t GetOffset(){
		return offset;
	}

	inline EdgeAttr* GetEdgeAttrs(EdgeAttr* base){
		return &(base[offset]);
	}

	inline size_t GetDegree(){
		return degree;
	}

	inline Edge& GetEdge(int index){
		assert(index < degree);
		return edges[index];
	}

	//todo : edge iterator
};

enum{
	EDGE_ONLY,
	EDGE_WITH_ONLY_PROB,
	EDGE_WITH_FULL_ATTRIBUTES,
	AUTO_DETECTED,
};

class Graph{
	uint8_t type;
	bool undirected;
	VertexId max_vertex_id;
	size_t num_edges;
	const size_t kBufSize = PAGE_SIZE;
	Edge* edges;
	off_t* index;
	EdgeAttr* edge_attrs;


public:
	Graph()
		: edges(NULL)
		, index(NULL)
		, edge_attrs(NULL)
		, type(EDGE_WITH_ONLY_PROB)
		, max_vertex_id(0)
		, num_edges(0)
		, undirected(false){
	};

	Graph(const char* fname): Graph(){
		Load(fname);
	}

	~Graph(){
		free(index);
		free(edges);
	}

	inline bool GetUnidrected(){
		return undirected;
	}

	inline Edge* GetOutEdges(){
		return edges;
	}

	inline Edge* GetInEdges(){
		unsigned int base = undirected ? 0: num_edges;
		return edges + base;
	}

	inline int GetType(){
		return type;
	}


	inline size_t GetOutDegree(int src){
		off_t offset = index[src];
		off_t next = index[src + 1];
		size_t len = next - offset;
		return len;
	}

	inline size_t GetInDegree(int dest){
		off_t base = undirected ? 0: max_vertex_id+1;
		off_t offset = index[dest + base];
		off_t next = index[dest + base + 1];
		size_t len = next - offset;
		assert(dest < max_vertex_id+1);
		assert(len < num_edges);
		return len;
	}
	inline std::pair<off_t, size_t> GetOutEdgeAddress(int src){
		off_t offset = index[src];
		off_t next = index[src + 1];
		size_t len = next - offset;
		assert(src < max_vertex_id+1);
		assert(len < num_edges);
		return {offset, len};
	}

	inline std::pair<off_t, size_t> GetInEdgeAddress(int dest){
		off_t base = undirected ? 0: max_vertex_id+1;
		off_t offset = index[dest + base];
		off_t next = index[dest + base + 1];
		size_t len = next - offset;
		assert(dest < max_vertex_id+1);
		assert(len < num_edges);
		return {offset, len};
	}

	inline EdgeAttr* GetAllEdgeAttrs(){
		return edge_attrs;
	}

	inline EdgeList GetOutEdgeList(int src){
		off_t offset = index[src];
		off_t next = index[src + 1];
		size_t len = next - offset;
		assert(src < max_vertex_id+1);
		assert(len < num_edges);

		return {&edges[offset], offset, len};
	}

	inline EdgeList GetInEdgeList(int dest){
		off_t base = undirected ? 0: max_vertex_id+1;
		off_t offset = index[dest + base];
		off_t next = index[dest + base + 1];
		size_t len = next - offset;
		assert(dest < max_vertex_id+1);
		assert(len < num_edges);
		return {&edges[offset], offset, len};
	}

	inline VertexId GetMaxVertexId(){
		return max_vertex_id;
	}

	inline size_t GetNumEdges(){
		return num_edges;
	}

	void Load(const char* fname){
		FILE *fp;

		if (!(fp = fopen(fname, "r"))) {
			fprintf(stderr, "can not load file\n");
			exit(-1);
		}

		if(index)
			free(index);
		if(edges)
			free(edges);

		size_t res = 0;
		res += fread(&type, sizeof(type), 1, fp);
		res += fread(&undirected, sizeof(undirected), 1, fp);
		res += fread(&max_vertex_id, sizeof(max_vertex_id), 1, fp);
		res += fread(&num_edges, sizeof(num_edges), 1, fp);

		if(undirected){
			index = (off_t *) malloc((max_vertex_id + 1 + 1) * sizeof(off_t));
			edges = (Edge *) malloc(num_edges * sizeof(Edge));
			res += fread(index, sizeof(off_t), max_vertex_id + 1 + 1, fp);
			res += fread(edges, sizeof(Edge), num_edges, fp);
			if (type == EDGE_WITH_FULL_ATTRIBUTES) {
				edge_attrs = (EdgeAttr *) malloc(num_edges * sizeof(EdgeAttr));
				res = fread(edge_attrs, sizeof(EdgeAttr), num_edges, fp);
			}
		}else {
			index = (off_t *) malloc(((max_vertex_id + 1) * 2 + 1) * sizeof(off_t));
			edges = (Edge *) malloc(num_edges * 2 * sizeof(Edge));
			res += fread(index, sizeof(off_t), (max_vertex_id + 1) * 2 + 1, fp);
			res += fread(edges, sizeof(Edge), num_edges * 2, fp);
			if (type == EDGE_WITH_FULL_ATTRIBUTES) {
				edge_attrs = (EdgeAttr *) malloc(num_edges * 2 * sizeof(EdgeAttr));
				res += fread(edge_attrs, sizeof(EdgeAttr), num_edges * 2, fp);
			}
		}

		printf("loaded: %lu\n", res);
		printf("type: %u\n", type);
		printf("undirected: %u\n", undirected);
		printf("max_vid: %u\n", max_vertex_id);
		printf("num_edges: %lu\n", num_edges);
	}

	void Save(const char* fname){
		FILE *fp;

		if (!(fp = fopen(fname, "w+"))) {
			fprintf(stderr, "can not save file\n");
			exit(-1);
		}

		fwrite(&type, sizeof(type), 1, fp);
		fwrite(&undirected, sizeof(undirected), 1, fp);
		fwrite(&max_vertex_id, sizeof(max_vertex_id), 1, fp);
		fwrite(&num_edges, sizeof(num_edges), 1, fp);

		if(undirected){
			fwrite(index, sizeof(off_t), max_vertex_id + 1 + 1, fp);
			fwrite(edges, sizeof(Edge), num_edges, fp);
			if (type == EDGE_WITH_FULL_ATTRIBUTES)
				fwrite(edge_attrs, sizeof(EdgeAttr), num_edges, fp);
		}else {
			fwrite(index, sizeof(off_t), (max_vertex_id + 1) * 2 + 1, fp);
			fwrite(edges, sizeof(Edge), num_edges * 2, fp);
			if (type == EDGE_WITH_FULL_ATTRIBUTES)
				fwrite(edge_attrs, sizeof(EdgeAttr), num_edges * 2, fp);
		}

		fclose(fp);
	}

	void LoadText(const char* filename, bool _undirected = false, int _type = AUTO_DETECTED) {
		std::unordered_map<VertexId, std::list<Edge> *> out_graph;
		std::unordered_map<VertexId, std::list<Edge> *> in_graph;
		VertexId src, dest;
		float prob = 0;

		std::unordered_map<VertexId, std::list<EdgeAttr> *> out_edge_attr;
		std::unordered_map<VertexId, std::list<EdgeAttr> *> in_edge_attr;
		EdgeAttr edge_attr;
		memset(&edge_attr, 0x00, sizeof(EdgeAttr));

		/* loading text */
		FILE *fp_graph;
		char buf[kBufSize];

		type = _type;
		undirected = _undirected;
		if (!(fp_graph = fopen(filename, "r"))) {
			fprintf(stderr, "can not open grpah file");
			exit(-1);
		}
		int line = -1;
		while (fgets(buf, PAGE_SIZE, fp_graph) != NULL) {
			line++;
			if (buf[0] == '#')
				continue;

			int res = sscanf(buf, "%d %d %f %f", &src, &dest, &prob, &edge_attr);
			if(type == AUTO_DETECTED){
				if(res >= 2)
					type = res - 2;
			}

			if (res != type + 2 && type < AUTO_DETECTED) {
				printf("%d : parsing error\n", line);
				continue;
			}

			if (src > max_vertex_id)
				max_vertex_id = src;
			if (dest > max_vertex_id)
				max_vertex_id = dest;

			if (out_graph.find(src) == out_graph.end()) {
				std::list<Edge> *adjlist = new std::list<Edge>();
				adjlist->push_back({dest, prob, ~0U});
				out_graph[src] = adjlist;

				if(type == EDGE_WITH_FULL_ATTRIBUTES) {
					std::list<EdgeAttr> *edge_attr_list = new std::list<EdgeAttr>();
					edge_attr_list->push_back(edge_attr);
					out_edge_attr[src] = edge_attr_list;
				}
			} else {
				out_graph[src]->push_back({dest, prob, ~0U});

				if(type == EDGE_WITH_FULL_ATTRIBUTES)
					out_edge_attr[src]->push_back(edge_attr);
			}

			if(undirected) {
				if(src != dest) {
					if (out_graph.find(dest) == out_graph.end()) {
						std::list<Edge> *adjlist = new std::list<Edge>();
						adjlist->push_back({src, prob, ~0U});
						out_graph[dest] = adjlist;

						if (type == EDGE_WITH_FULL_ATTRIBUTES) {
							std::list<EdgeAttr> *edge_attr_list = new std::list<EdgeAttr>();
							edge_attr_list->push_back(edge_attr);
							out_edge_attr[dest] = edge_attr_list;
						}
					} else {
						out_graph[dest]->push_back({src, prob, ~0U});

						if (type == EDGE_WITH_FULL_ATTRIBUTES)
							out_edge_attr[dest]->push_back(edge_attr);
					}
					num_edges++;
				}
			} else {
				if (in_graph.find(dest) == in_graph.end()) {
					std::list<Edge> *adjlist = new std::list<Edge>();
					adjlist->push_back({src, prob, ~0U});
					in_graph[dest] = adjlist;

					if (type == EDGE_WITH_FULL_ATTRIBUTES) {
						std::list<EdgeAttr> *edge_attr_list = new std::list<EdgeAttr>();
						edge_attr_list->push_back(edge_attr);
						in_edge_attr[dest] = edge_attr_list;
					}
				} else {
					in_graph[dest]->push_back({src, prob, ~0U});

					if (type == EDGE_WITH_FULL_ATTRIBUTES)
						in_edge_attr[dest]->push_back(edge_attr);
				}
			}

			num_edges++;
		}
		fclose(fp_graph);

		if(undirected){
			index = (off_t *) malloc((max_vertex_id + 1 + 1) * sizeof(off_t));
			edges = (Edge *) malloc(num_edges * sizeof(Edge));
			if (type == EDGE_WITH_FULL_ATTRIBUTES)
				edge_attrs = (EdgeAttr *) malloc(num_edges * sizeof(EdgeAttr));
			memset(index, ~0, ((max_vertex_id + 1) + 1) * sizeof(off_t));
			memset(edges, ~0, (num_edges * sizeof(off_t)));

			if (type == EDGE_WITH_FULL_ATTRIBUTES)
				memset(edge_attrs, 0, (num_edges * sizeof(EdgeAttr)));

		}else {
			index = (off_t *) malloc(((max_vertex_id + 1) * 2 + 1) * sizeof(off_t));
			edges = (Edge *) malloc(num_edges * 2 * sizeof(Edge));
			if (type == EDGE_WITH_FULL_ATTRIBUTES)
				edge_attrs = (EdgeAttr *) malloc(num_edges * 2 * sizeof(EdgeAttr));
			memset(index, ~0, ((max_vertex_id + 1) * 2 + 1) * sizeof(off_t));
			memset(edges, ~0, (num_edges * 2 * sizeof(off_t)));

			if (type == EDGE_WITH_FULL_ATTRIBUTES)
				memset(edge_attrs, 0, (num_edges * 2 * sizeof(EdgeAttr)));
		}
		int cursor = 0;
		int cursor2 = 0;
		index[0] = 0;

		for (int i = 0; i < max_vertex_id + 1; i++) {
			auto tmp = out_graph.find(i);

			if (tmp == out_graph.end()) {
				index[i] = cursor;
				continue;
			} else {
				std::list<Edge> *list = tmp->second;

				cursor2 = cursor;
				for (auto iter2 = list->begin(); iter2 != list->end(); iter2++) {
					if (iter2 == list->begin()) {
						index[i] = cursor;
					}
					edges[cursor] = *iter2;
					cursor++;
				}

				if(type ==  EDGE_WITH_FULL_ATTRIBUTES) {
					auto tmp = out_edge_attr.find(i);
					std::list<EdgeAttr> *list2 = tmp->second;

					assert(list2->size() == list->size());
					for (auto iter2 = list2->begin(); iter2 != list2->end(); iter2++) {
						edge_attrs[cursor2] = *iter2;
						cursor2++;
					}
					assert(cursor == cursor2);
				}
			}
		}

		if(!undirected) {
			for (int i = 0; i < max_vertex_id + 1; i++) {
				auto tmp = in_graph.find(i);

				if (tmp == in_graph.end()) {
					index[i + max_vertex_id + 1] = cursor;
					continue;
				} else {
					std::list<Edge> *list = tmp->second;

					cursor2 = cursor;
					for (auto iter2 = list->begin(); iter2 != list->end(); iter2++) {
						if (iter2 == list->begin()) {
							index[i + max_vertex_id + 1] = cursor;
						}
						edges[cursor] = *iter2;
						cursor++;
					}

					if (type == EDGE_WITH_FULL_ATTRIBUTES) {
						auto tmp = in_edge_attr.find(i);
						std::list<EdgeAttr> *list2 = tmp->second;

						assert(list2->size() == list->size());
						for (auto iter2 = list2->begin(); iter2 != list2->end(); iter2++) {
							edge_attrs[cursor2] = *iter2;
							cursor2++;
						}
						assert(cursor == cursor2);
					}
				}
			}
		}

		if(undirected)
			index[max_vertex_id+1] = cursor;
		else
			index[(max_vertex_id+1) * 2] = cursor;

		// pair edge
		for(int i = 0; i < max_vertex_id + 1; i++){
			VertexId dest = i;
			EdgeList src_list = GetInEdgeList(dest);
			for(int j=0; j<src_list.GetDegree(); j++){
				Edge& edge = src_list.GetEdge(j);
				VertexId src = edge.neighbor;

				EdgeList dest_list = GetOutEdgeList(src);
				for(int k=0; k<dest_list.GetDegree()-1; k++){
					VertexId next_dest = ~0U;
					if(dest == dest_list.GetEdge(k).neighbor){
						next_dest = dest_list.GetEdge(k+1).neighbor;
						edge.next_dest = next_dest;
						break;
					}
				}
			}
		}

		// edge attribute
		for (int i = 0; i < max_vertex_id + 1; i++) {
			auto tmp = out_graph.find(i);

			if (tmp == out_graph.end())
				continue;

			std::list<Edge> *list = tmp->second;
			delete (list);

			if(type == EDGE_WITH_FULL_ATTRIBUTES){
				auto tmp = out_edge_attr.find(i);

				if (tmp == out_edge_attr.end())
					continue;

				std::list<EdgeAttr> *list = tmp->second;
				delete (list);
			}
		}
		out_graph.clear();
		if(type == EDGE_WITH_FULL_ATTRIBUTES)
			out_edge_attr.clear();

		if(!undirected) {
			for (int i = 0; i < max_vertex_id + 1; i++) {
				auto tmp = in_graph.find(i);

				if (tmp == in_graph.end())
					continue;

				std::list<Edge> *list = tmp->second;
				delete (list);

				if (type == EDGE_WITH_FULL_ATTRIBUTES) {
					auto tmp = in_edge_attr.find(i);

					if (tmp == in_edge_attr.end())
						continue;

					std::list<EdgeAttr> *list = tmp->second;
					delete (list);
				}
			}
			in_graph.clear();

			if (type == EDGE_WITH_FULL_ATTRIBUTES)
				in_edge_attr.clear();
		}
	}

	void PrintIndex(bool out_edge = true) {
		unsigned int begin;
		if(out_edge)
			begin = 0;
		else
			begin = max_vertex_id + 1;

		for (int i = begin; i < begin + max_vertex_id + 1; i++) {
			printf("%d: %lu, %lu\n", i, index[i], index[i+1]-index[i]);
		}
	}

	void PrintEdgeList(bool out_edge = true){
		for(int i=0; i<100+1; i++) {
			if(out_edge) {
				EdgeList edges = GetOutEdgeList(i);
				size_t degree = edges.GetDegree();
				EdgeAttr* edge_attrs = edges.GetEdgeAttrs(GetAllEdgeAttrs());

				for (int j = 0; j < edges.GetDegree(); j++) {
					Edge& edge = edges.GetEdge(j);
					EdgeAttr edge_attr = 0;
					float probability = 0;
					if(type >=  EDGE_WITH_ONLY_PROB)
						probability = edge.probability;
					if(type == EDGE_WITH_FULL_ATTRIBUTES)
						edge_attr = edge_attrs[j];

					printf("%u %u %f %f\n", i, edge.neighbor, probability, edge_attr);
				}
			}else{
				EdgeList edges = GetInEdgeList(i);
				size_t degree = edges.GetDegree();
				EdgeAttr* edge_attrs = edges.GetEdgeAttrs(GetAllEdgeAttrs());

				for (int j = 0; j < edges.GetDegree(); j++) {
					Edge& edge = edges.GetEdge(j);
					EdgeAttr edge_attr = 0;
					float probability = 0;
					if(type >=  EDGE_WITH_ONLY_PROB)
						probability = edge.probability;
					if(type == EDGE_WITH_FULL_ATTRIBUTES)
						edge_attr = edge_attrs[j];

					printf("%u %u %f %f\n", i, edge.neighbor, probability, edge_attr);
				}
			}
		}
	}
};

#endif //SAGE_EDGE_MANAGER_HPP
