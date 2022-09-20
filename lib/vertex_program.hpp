#ifndef SAGE_VERTEX_PROGRAM_HPP
#define SAGE_VERTEX_PROGRAM_HPP

#include <random>
#include "bitmap.hpp"
#include "graph.hpp"
#include "vertex.hpp"
#include "serialized_value_bitmap.hpp"

template <typename VertexType>
class VertexProgramIteratorThread;

template <typename VertexType>
class GraphEngine;

template<typename VertexType>
class VertexProgram{
	Graph* ptr_graph;
	VertexProgramIteratorThread<VertexType>* ptr_vertex_program_iterator_thread;
	unsigned int sample_process_mode;
	bool use_activated_src_bitmap;

	std::random_device rd;
	std::default_random_engine random_engine;
	std::uniform_real_distribution<float> uniform_dist;
	size_t cnt_transform;

public:
	VertexProgram(){
		uniform_dist = std::uniform_real_distribution<float>(0.0, 1.0);
		random_engine = std::default_random_engine(rd());
		size_t src_buf_size = SerializedBundleVertex<VertexType>::GetAllocPageUnit(NUM_SAMPLE) * PAGE_SIZE;
		cnt_transform = 0;
	}

	~VertexProgram(){
	    GetGraphEngine()->SumStat(0, 0, 0, 0, 1, cnt_transform);
	}

	void Init(Graph* ptr_graph, VertexProgramIteratorThread<VertexType>* ptr_vertex_program_iterator_thread, unsigned int sample_process_mode){
		this->ptr_graph = ptr_graph;
		this->ptr_vertex_program_iterator_thread = ptr_vertex_program_iterator_thread;
		this->sample_process_mode = sample_process_mode;

	}

    bool _GatherByDest(void** _ptr_src_buf, void** _ptr_dest_buf, Edge& edge, Bitmap* ptr_sampling_bitmap, EdgeAttr edge_attr,  size_t* ptr_dest_buf_size) {
		VertexId src_id = GetCurrentSrcId();
		VertexId dest_id = GetCurrentDestId();
		bool activate_dest = false;
        int src_vertex_type = *((int*) ((uint8_t*)(*_ptr_src_buf) + sizeof(VertexId)));
        int dest_vertex_type = *((int*) ((uint8_t*)(*_ptr_dest_buf) + sizeof(VertexId)));

        if(sample_process_mode == 3 && ((src_vertex_type == 1 && dest_vertex_type == 0) || (src_vertex_type ==0 && dest_vertex_type == 1))) {
            if (src_vertex_type == 1) {
                SerializedSimVertex<VertexType> sim_src_vertex(src_id, NUM_SAMPLE, _ptr_src_buf);
                SerializedBundleVertex<VertexType> dest_bundle_vertex(dest_id, NUM_SAMPLE, *_ptr_dest_buf);

                int iteration = GetGraphEngine()->GetCurrentIteration();
                Bitmap next_activated_dest_bitmap = dest_bundle_vertex.GetActivationBitmap(
                        (iteration + 1) % 2);
                Bitmap current_activated_src_bitmap = sim_src_vertex.GetActivationBitmap(
                        iteration % 2);

                for(int i=0; i<sim_src_vertex.GetNumValue(); i++){
                    auto value_bitmap = sim_src_vertex.GetValueBitmapByIndex(i);
                    for(int j=0; j<NUM_SAMPLE; j++){
                        if(value_bitmap.second.Get(j)){
                            if (!current_activated_src_bitmap.Get(j))
                                continue;
                            if (ptr_sampling_bitmap->Get(j)) {
                                VertexType *ptr_src_vertex = value_bitmap.first;

                                bool _activate_dest = GatherByDest(*ptr_src_vertex,
                                                                   *(dest_bundle_vertex.GetVertexAttributeByIndex(i)), edge,
                                                                   edge_attr);
                                if (_activate_dest)
                                    ActivateBundleVertex(dest_id, i, &next_activated_dest_bitmap);
                                activate_dest |= _activate_dest;
                            }
                        }
                    }
                    return activate_dest;
                }
            } else if (dest_vertex_type == 1) {
                SerializedSimVertex<VertexType> sim_dest_vertex(dest_id, NUM_SAMPLE, _ptr_dest_buf);
                sim_dest_vertex.TransformSelf();
                cnt_transform++;
//                printf("Transform %p %d %d %d\n", *_ptr_dest_buf, sim_dest_vertex.GetVertexId(), *((VertexId*)*_ptr_dest_buf), dest_id);
                *ptr_dest_buf_size = SerializedBundleVertex<VertexType>::GetAllocPageUnit(NUM_SAMPLE) * PAGE_SIZE;
                dest_vertex_type = *((int*) ((uint8_t*)(*_ptr_dest_buf) + sizeof(VertexId)));
            } else {
                assert(0);
            }
        }
        if(sample_process_mode == 1 || (sample_process_mode == 3 && src_vertex_type == 0 && dest_vertex_type == 0)) {
            SerializedBundleVertex<VertexType> src_bundle_vertex(src_id, NUM_SAMPLE, *_ptr_src_buf);
            SerializedBundleVertex<VertexType> dest_bundle_vertex(dest_id, NUM_SAMPLE, *_ptr_dest_buf);
            int iteration = GetGraphEngine()->GetCurrentIteration();
            Bitmap next_activated_dest_bitmap = dest_bundle_vertex.GetActivationBitmap(
                    (iteration + 1) % 2);
            Bitmap current_activated_src_bitmap = src_bundle_vertex.GetActivationBitmap(
                    iteration % 2);
            for (unsigned int i = 0; i < NUM_SAMPLE; i++) {
                if (!current_activated_src_bitmap.Get(i))
                    continue;
                if (ptr_sampling_bitmap->Get(i)) {
                    bool _activate_dest = GatherByDest(*(src_bundle_vertex.GetVertexAttributeByIndex(i)),
                                                       *(dest_bundle_vertex.GetVertexAttributeByIndex(i)), edge,
                                                       edge_attr);
                    if (_activate_dest)
                        ActivateBundleVertex(dest_id, i, &next_activated_dest_bitmap);
                    activate_dest |= _activate_dest;
                }
            }
		}else if(sample_process_mode == 2 || (sample_process_mode == 3 && src_vertex_type == 1 && dest_vertex_type == 1)){
			SerializedSimVertex<VertexType> sim_src_vertex(src_id, NUM_SAMPLE, _ptr_src_buf);
			SerializedSimVertex<VertexType> sim_dest_vertex(dest_id, NUM_SAMPLE, _ptr_dest_buf);

			int iteration = GetGraphEngine()->GetCurrentIteration();
			Bitmap next_activated_dest_bitmap = sim_dest_vertex.GetActivationBitmap(
					(iteration + 1) % 2);
			Bitmap current_activated_src_bitmap = sim_src_vertex.GetActivationBitmap(
					iteration % 2);
			std::vector<std::pair<VertexType, Bitmap>> insert_list;
			insert_list.reserve(sim_dest_vertex.GetNumValue()*sim_src_vertex.GetNumValue());

			ptr_vertex_program_iterator_thread->AddProgramIter(sim_dest_vertex.GetNumValue()*sim_src_vertex.GetNumValue());

			size_t bitmap_size = sizeof(Bit64) *  BaseBitmap::GetArraySize(NUM_SAMPLE);
			size_t s = ((bitmap_size * sim_dest_vertex.GetNumValue() * sim_src_vertex.GetNumValue() + sizeof(Bit64) - 1) / sizeof(Bit64)) * sizeof(Bit64);
//					printf("%d %d %d %d\n",s, bitmap_size, sim_src_vertex.GetNumValue(), sim_dest_vertex.GetNumValue());
			//uint8_t base_bitmap[s];
			uint8_t *base_bitmap = (uint8_t*) malloc(s);
			memset(base_bitmap, 0xff, s);

			struct timeval start, end;
			gettimeofday(&start, 0);

			for (int i=0; i<sim_dest_vertex.GetNumValue(); i++){
				VertexType _dest = *(sim_dest_vertex.GetValueBitmapByIndex(i).first);
				Bitmap dest_bitmap = sim_dest_vertex.GetValueBitmapByIndex(i).second;

				for(int j=0; j<sim_src_vertex.GetNumValue(); j++){

					VertexType dest = _dest;
					VertexType src = *(sim_src_vertex.GetValueBitmapByIndex(j).first);
					Bitmap src_bitmap = sim_src_vertex.GetValueBitmapByIndex(j).second;

					uint8_t* ptr_base_bitmap = base_bitmap + (i*sim_src_vertex.GetNumValue() + j) * bitmap_size;
					Bitmap new_bitmap((Bit64* )ptr_base_bitmap, NUM_SAMPLE);
					// bfs hop update
					new_bitmap.And(dest_bitmap);
					new_bitmap.And(src_bitmap);
					new_bitmap.And(*ptr_sampling_bitmap);
					new_bitmap.And(current_activated_src_bitmap);

					if (new_bitmap.IsAllClear()) {
						continue;
					}
					VertexType dest_bak = dest;
					bool _activate_dest = GatherByDest(src, dest, edge, edge_attr);

					if (_activate_dest)
						ActivateSimVertex(dest_id, new_bitmap, &next_activated_dest_bitmap);
					activate_dest |= _activate_dest;

					if (dest_bak == dest) {
						new_bitmap.ClearAll();
						continue;
					}

					insert_list.push_back({dest, new_bitmap});
				}

				for (int k = 0; k < insert_list.size(); k++) {
					sim_dest_vertex.GetValueBitmapByIndex(i).second.Exclude(insert_list[k].second);
				}

			}
			gettimeofday(&end, 0);

			gettimeofday(&start, 0);
			for (int i = 0; i < insert_list.size(); i++) {
				VertexType dest = insert_list[i].first;
				Bitmap new_bitmap = insert_list[i].second;

				if(new_bitmap.IsAllClear())
					continue;

				auto it = sim_dest_vertex.Find(dest);
				if(it.first == NULL){
					bool is_expand;
					auto entry = sim_dest_vertex.AllocNewValue(&is_expand);
					if(is_expand){
						*ptr_dest_buf_size = sim_dest_vertex.GetNumAllocatedPage() * PAGE_SIZE;
					}
					*(entry.first) = dest;
					entry.second.Or(new_bitmap);
				}else{
					assert(*(it.first) == dest);
					it.second.Or(new_bitmap);
				}
			}
//	        sim_dest_vertex.Check();
			if(sample_process_mode == 3) {
                if (sim_dest_vertex.GetNumAllocatedPage() > (SerializedBundleVertex<VertexType>::GetAllocPageUnit(NUM_SAMPLE) ) ) {
//                    printf("Transform before %p %d %d %d\n", *_ptr_dest_buf, sim_dest_vertex.GetNumAllocatedPage(), SerializedBundleVertex<VertexType>::GetAllocPageUnit(NUM_SAMPLE), sim_dest_vertex.GetNumValue());
                    sim_dest_vertex.TransformSelf();
                    cnt_transform++;
//                    printf("Transform %p %d %d %d\n", *_ptr_dest_buf, sim_dest_vertex.GetVertexId(), *((VertexId*)*_ptr_dest_buf), dest_id);
                    *ptr_dest_buf_size = SerializedBundleVertex<VertexType>::GetAllocPageUnit(NUM_SAMPLE) * PAGE_SIZE;
                }
            }
			free(base_bitmap);
			gettimeofday(&end, 0);
        }else{
            printf("%d %d %d\n", sample_process_mode, src_vertex_type, dest_vertex_type);
            assert(0);
		}
		return activate_dest;
	}

	virtual bool GatherByDest(VertexType& src, VertexType& dest, Edge& edge, EdgeAttr edge_attr) = 0;

	void _ScatterBySrc(void** ptr_src_buf, EdgeList out_edge_list, EdgeList in_edge_list, size_t* ptr_src_buf_size){
		bool is_scatter = false;
		VertexId src_id = GetCurrentSrcId();

		int iteration = GetGraphEngine()->GetCurrentIteration();
        int vertex_type = *((int*) ((uint8_t*)(*ptr_src_buf) + sizeof(VertexId)));

		//todo override 여부로 확인 돌릴지말지 확인
		if(sample_process_mode == 1 || (sample_process_mode == 3 && vertex_type == 0)){
			SerializedBundleVertex<VertexType> src_bundle_vertex(src_id, NUM_SAMPLE, *ptr_src_buf);
			Bitmap activate_bitmap = src_bundle_vertex.GetActivationBitmap(iteration % 2);
			Bitmap next_bitmap = src_bundle_vertex.GetActivationBitmap((iteration+1) % 2);
			next_bitmap.ClearAll();
			for (int i = 0; i < NUM_SAMPLE; i++) {
				if (!activate_bitmap.Get(i))
					continue;
				VertexType *ptr_vertex = src_bundle_vertex.GetVertexAttributeByIndex(i);
				bool _is_scatter = ScatterBySrc(*ptr_vertex, out_edge_list, in_edge_list);
				if (!_is_scatter)
					activate_bitmap.Clear(i);
				is_scatter |= _is_scatter;

			}
		}else if(sample_process_mode == 2 || (sample_process_mode == 3 && vertex_type == 1)){
			SerializedSimVertex<VertexType> sim_vertex(src_id, NUM_SAMPLE, ptr_src_buf);
			Bitmap activate_bitmap = sim_vertex.GetActivationBitmap(iteration % 2);
			Bitmap next_bitmap = sim_vertex.GetActivationBitmap((iteration+1) % 2);
			next_bitmap.ClearAll();

			std::vector<std::pair<VertexType, Bitmap *>> insert_list;
			insert_list.reserve(sim_vertex.GetNumValue());


			for(int i=0; i<sim_vertex.GetNumValue(); i++){
				VertexType v = *(sim_vertex.GetValueBitmapByIndex(i).first);
				Bitmap bitmap = sim_vertex.GetValueBitmapByIndex(i).second;

				Bitmap *ptr_new_bitmap = new Bitmap(NUM_SAMPLE);
				ptr_new_bitmap->SetAll();
				ptr_new_bitmap->And(bitmap);
				ptr_new_bitmap->And(activate_bitmap);

				if (ptr_new_bitmap->IsAllClear()) {
					delete ptr_new_bitmap;
					continue;
				}
				VertexType v_bak = v;

				bool _is_scatter = ScatterBySrc(v, out_edge_list, in_edge_list);

				if (!_is_scatter)
					activate_bitmap.Exclude(*ptr_new_bitmap);
				is_scatter |= _is_scatter;

				if(v_bak == v && ptr_new_bitmap->IsEqual(bitmap)){
					delete(ptr_new_bitmap);
					continue;
				}

				insert_list.push_back({v, ptr_new_bitmap});
				sim_vertex.GetValueBitmapByIndex(i).second.Exclude(*ptr_new_bitmap);
			}

			for (int i = 0; i < insert_list.size(); i++) {
				VertexType v = insert_list[i].first;
				Bitmap *ptr_new_bitmap = insert_list[i].second;

				auto it = sim_vertex.Find(v);
				if(it.first == NULL){
					bool is_expand;
					auto entry = sim_vertex.AllocNewValue(&is_expand);
					if(is_expand){
						*ptr_src_buf_size = sim_vertex.GetNumAllocatedPage() * PAGE_SIZE;
					}
					*(entry.first) = v;
					entry.second.Or(*ptr_new_bitmap);
//					assert(is_expand == false);
				}else{
					assert(*(it.first) == v);
					it.second.Or(*ptr_new_bitmap);
				}
				delete(ptr_new_bitmap);
			}
//			sim_vertex.Check();
			GetThread()->AddSumMapSize(sim_vertex.GetNumValue());
        }

		if(is_scatter){
			ScatterOutEdgeList(out_edge_list);
		}
	};

	virtual bool ScatterBySrc(VertexType& src, EdgeList& out_edge_list, EdgeList& in_edge_list){
		return true;
	};

	VertexProgramIteratorThread<VertexType>* GetThread(){
		return ptr_vertex_program_iterator_thread;
	}

	GraphEngine<VertexType>* GetGraphEngine(){
		return GetThread()->GetGraphEngine();
	}

	void ActivateVertex(VertexId v) {
		//ptr_vertex_program_iterator_thread->CountActivation();
		ptr_vertex_program_iterator_thread->GetActivatedVertex()->Set(v);
	}

	void ActivateBundleVertex(VertexId v, unsigned int sample, Bitmap* activated_dest_bitmap) {
		activated_dest_bitmap->Set(sample);
	}

	void ActivateSimVertex(VertexId v, Bitmap& bitmap, Bitmap* activated_dest_bitmap){
		activated_dest_bitmap->Or(bitmap);
	}

	void ActivateEdgeList(EdgeList edge_list) {
		size_t degree = edge_list.GetDegree();
		Bitmap* ptr_activated_vertex = ptr_vertex_program_iterator_thread->GetActivatedVertex();
		for(size_t i=0; i<degree; i++)
			ptr_activated_vertex->Set(edge_list.GetEdge(i).neighbor);
		ptr_vertex_program_iterator_thread->CountActivation(degree);
	}

	Bitmap* GetActivatedVertex() {
		return ptr_vertex_program_iterator_thread->GetActivatedVertex();
	}

	Graph* GetGraph() {
		return ptr_graph;
	}

	inline VertexId GetCurrentSrcId(){
		return GetThread()->GetCurrentSrcId();
	}

	inline VertexId GetCurrentDestId(){
		return GetThread()->GetCurrentDestId();
	}

	void ScatterOutEdgeList(EdgeList edge_list){
		VertexId src_id = GetCurrentSrcId();
		Bitmap* ptr_scattered_vertex = ptr_vertex_program_iterator_thread->GetScatteredVertex();
		ptr_scattered_vertex->Set(src_id);
		size_t degree = edge_list.GetDegree();

		Bitmap* ptr_scattered_out_neighbor = ptr_vertex_program_iterator_thread->GetScatteredOutNeighbor();
		for(size_t i=0; i<degree; i++) {
			ptr_scattered_out_neighbor->Set(edge_list.GetEdge(i).neighbor);
		}
	}

	void ScatterInEdgeList(VertexId dest_id, EdgeList edge_list){
		Bitmap* ptr_scattered_vertex = ptr_vertex_program_iterator_thread->GetScatteredVertex();
		ptr_scattered_vertex->Set(dest_id);
		size_t degree = edge_list.GetDegree();

		Bitmap* ptr_scattered_in_neighbor = ptr_vertex_program_iterator_thread->GetScatteredInNeighbor();
		for(size_t i=0; i<degree; i++)
			ptr_scattered_in_neighbor->Set(edge_list.GetEdge(i).neighbor);
	}

	inline off_t GetInEdgeOffset(Edge* edge){
		off_t res = edge - ptr_graph->GetInEdges();
		assert(res <= ptr_graph->GetNumEdges());
		return res;
	}

	inline off_t GetOutEdgeOffset(Edge* edge){
		off_t res = edge - ptr_graph->GetOutEdges();
		assert(res <= ptr_graph->GetNumEdges());
		return res;
	}
};

#endif //SAGE_VERTEX_PROGRAM_HPP
