#ifndef SAGE_MULTI_LEVEL_LRU_HPP
#define SAGE_MULTI_LEVEL_LRU_HPP

#include <list>
#include <unordered_map>
#include <cassert>
#include <cstdio>
#include "vertex.hpp"

class MultiSectionLru {
	std::list<std::pair<VertexId, double>>* array_lru_list;
	typedef typename std::list<std::pair<VertexId, double>>::iterator LruIterator;
	std::pair<int, LruIterator>* lru_map;
	size_t num_vertex;
	size_t num_section;
	pthread_spinlock_t lock;
	pthread_spinlock_t* vertex_lock;
	int max_section;

public:
	MultiSectionLru(size_t num_vertex, size_t num_section) :
			num_vertex(num_vertex),
			num_section(num_section){
		vertex_lock = NULL;
		assert(num_vertex > 0);
		assert(num_section >= 2);
		pthread_spin_init(&lock, 0);
		array_lru_list = new std::list<std::pair<VertexId, double>>[num_section]();
		lru_map = new std::pair<int, LruIterator>[num_vertex];
		for(int i=0; i<num_vertex; i++)
			lru_map[i] = {0, array_lru_list[0].end()};
		max_section = 0;
	}

	~MultiSectionLru(){
		delete[] array_lru_list;
		delete[] lru_map;
	}

	void Init(){
		delete[] array_lru_list;
		delete[] lru_map;

		vertex_lock = NULL;
		assert(num_vertex > 0);
		assert(num_section >= 2);
		pthread_spin_init(&lock, 0);
		array_lru_list = new std::list<std::pair<VertexId, double>>[num_section]();
		lru_map = new std::pair<int, LruIterator>[num_vertex];
		for(int i=0; i<num_vertex; i++)
			lru_map[i] = {0, array_lru_list[0].end()};
		max_section = 0;
	}

	void SetVertexLock(pthread_spinlock_t* vertex_lock){
		this->vertex_lock = vertex_lock;
	}

	void Cleaning(int prev_section, int current_section){
		pthread_spin_lock(&lock);
		for(int i=prev_section; i<current_section; i++){
			while(array_lru_list[i].size()) {
				auto tmp = array_lru_list[i].front();
				_Access(tmp.first, 0, tmp.second, false);
			}
		}
		pthread_spin_unlock(&lock);
	}

	size_t GetSize(){
		size_t size = 0;
		for(int i=0; i<num_section; i++)
			size += array_lru_list[i].size();
		return size;
	}

	void _Access(const VertexId vertex, int section, double priority, bool new_vertex) {
		int prev_section = lru_map[vertex].first;
		if(section < 0)
			section = prev_section;

		if (lru_map[vertex].second != array_lru_list[prev_section].end()){
			array_lru_list[prev_section].erase(lru_map[vertex].second);
		}

		array_lru_list[section].push_front({vertex, priority});

		if(section > max_section)
			max_section = section;

		lru_map[vertex] = {section, array_lru_list[section].begin()};
	}


	void Access(const VertexId vertex, int section, double priority, bool new_vertex) {
		pthread_spin_lock(&lock);
		_Access(vertex, section, priority, new_vertex);
		pthread_spin_unlock(&lock);
	}

	VertexId _GetVictim(const int section){
		if(array_lru_list[section].size() < 1)
			return ~0U;
		auto victim = array_lru_list[section].end();
		victim--;
		VertexId ret = victim->first;
		lru_map[ret] = {0, array_lru_list[0].end()};
		array_lru_list[section].pop_back();

		return ret;
	}

	VertexId GetVictim(int current_section) {
		pthread_spin_lock(&lock);
		if(array_lru_list[0].size()){
			VertexId ret = _GetVictim(0);
			pthread_spin_unlock(&lock);
			return ret;
		}
		max_section = num_section - 1;
		while(array_lru_list[max_section].size() == 0) {
			max_section--;
			//assert(max_section >= current_section);
			assert(max_section >= 1);
		}


		assert(max_section >= 1);
		VertexId ret = _GetVictim(max_section);
		pthread_spin_unlock(&lock);
		return ret;
	}

	void Print(){
		for(int i=0; i<num_section; i++){
			if(array_lru_list[i].size())
				printf("%d(%d): ", i, array_lru_list[i].size());
		}
		printf("\n");
	}
};
#endif //SAGE_MULTI_LEVEL_LRU_HPP
