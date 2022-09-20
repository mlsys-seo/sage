#ifndef SAGE_SERIALIZED_VALUE_BITMAP_HPP
#define SAGE_SERIALIZED_VALUE_BITMAP_HPP

#include "bitmap.hpp"
#include "aio.hpp"
#include <tuple>
#include "vertex.hpp"
#include <algorithm>
#include "config.hpp"


template<typename Value>
class SerializedBundleVertex{
    int* ptr_type;
	VertexId vertex_id;
	void* data;
	VertexId* ptr_vertex_id;
	unsigned int* ptr_num_sample;   //padding
	void* ptr_activated_sample_bitmap_data[2];
	void* ptr_vertex_array;
	unsigned int alloc_page_unit;

public:
	void PtrInit(void* data, unsigned int num_sample){
        ptr_vertex_id = (VertexId*) (this->data);
	    ptr_type = (int*) (((uint8_t*)ptr_vertex_id) + sizeof(VertexId));
		ptr_num_sample = (unsigned int*) (((uint8_t*) ptr_type) + sizeof(int));
		ptr_activated_sample_bitmap_data[0] = ((uint8_t*) ptr_num_sample) + sizeof(unsigned int);
		ptr_activated_sample_bitmap_data[1] = ((uint8_t*) ptr_activated_sample_bitmap_data[0]) + BaseBitmap::GetArraySize(num_sample)*sizeof(uint64_t);
		ptr_vertex_array = ((uint8_t*) ptr_activated_sample_bitmap_data[1]) + BaseBitmap::GetArraySize(num_sample) * sizeof(uint64_t);
	}

	//todo ...
	static size_t GetAllocPageUnit(unsigned int num_sample){
		size_t ret = (sizeof(int) + sizeof(VertexId) + sizeof(unsigned int) + BaseBitmap::GetArraySize(num_sample) * sizeof(uint64_t) * 2 + sizeof(Value) * num_sample + PAGE_SIZE - 1) / PAGE_SIZE;
		return ret;
	}

	SerializedBundleVertex(VertexId vertex_id, unsigned int num_sample, void* data) {
		alloc_page_unit = GetAllocPageUnit(num_sample);

		assert(data != NULL);
		this->data = data;

		PtrInit(this->data, num_sample);
		this->vertex_id = vertex_id;
		if(vertex_id == ~0U) {
            *ptr_type = 0;
			*ptr_vertex_id = vertex_id;
			*ptr_num_sample = num_sample;
		}
	}
	~SerializedBundleVertex(){

	}

	void SetVertexId(VertexId vertex_id){
	    this->vertex_id = vertex_id;
	    *ptr_vertex_id = vertex_id;
	}

	static void PrintSize(unsigned int num_sample){
		size_t vertex_array_size = sizeof(int) + sizeof(VertexId) + sizeof(unsigned int) + sizeof(unsigned int)*2 + BaseBitmap::GetArraySize(num_sample) * sizeof(uint64_t) * 2 + num_sample*sizeof(Value);
		printf("value_array_size: %d\n", vertex_array_size);
		printf("value_array_page_num: %d\n", (vertex_array_size + PAGE_SIZE-1)/ PAGE_SIZE);
	}
	Bitmap GetActivationBitmap(unsigned int idx){
		assert(idx<2);
		return Bitmap((uint64_t*) ptr_activated_sample_bitmap_data[idx], *ptr_num_sample);
	}

	VertexId GetVertexId(){
		return vertex_id;
	}

	std::pair<void*, unsigned int> GetData(){
		return {data, alloc_page_unit};
	}

	Value* GetVertexAttributeByIndex(unsigned int idx){
		return (Value*) (((uint8_t*) ptr_vertex_array) + sizeof(Value) * idx);
	}

	Value* GetVertexAttributeArray(){
		return (Value*) (((uint8_t*) ptr_vertex_array));
	}
};

template<typename Value>
class SerializedSimVertex{
    int* ptr_type;
	void** ptr_data;
	VertexId* ptr_vertex_id;
	unsigned int* ptr_num_allocated_page;
	unsigned int* ptr_num_value;
	unsigned int* ptr_num_sample;   //padding
	void* ptr_activated_sample_bitmap_data[2];
	void* ptr_value_bitmap;
	VertexId vertex_id;
	unsigned int entry_size;
	unsigned int alloc_page_unit;
	size_t value_size;

public:
	void PtrInit(void* data, unsigned int num_sample){
		*(this->ptr_data) = data;
        ptr_vertex_id = (VertexId*) (*this->ptr_data);
        ptr_type = (int*) (((uint8_t*)ptr_vertex_id) + sizeof(VertexId));
		ptr_num_allocated_page = (unsigned int*)(((uint8_t*) ptr_type) + sizeof(int));
		ptr_num_value = (unsigned int*)(((uint8_t*) ptr_num_allocated_page) + sizeof(unsigned int));
		ptr_num_sample = (unsigned int*) (((uint8_t*) ptr_num_value) + sizeof(unsigned int));
		ptr_activated_sample_bitmap_data[0] = ((uint8_t*) ptr_num_sample) + sizeof(unsigned int);
		ptr_activated_sample_bitmap_data[1] = ((uint8_t*) ptr_activated_sample_bitmap_data[0]) + BaseBitmap::GetArraySize(num_sample)*sizeof(uint64_t);
		ptr_value_bitmap = ((uint8_t*) ptr_activated_sample_bitmap_data[1]) + BaseBitmap::GetArraySize(num_sample) * sizeof(uint64_t);
	}

	static void PrintSize(unsigned int num_sample){
		size_t value_size = sizeof(Value);
		size_t entry_size = sizeof(uint64_t) * BaseBitmap::GetArraySize(num_sample) + value_size;
		printf("value_size: %d\n", sizeof(Value));
		printf("entry_size: %d\n", entry_size);
		printf("memory per vertex: %d\n", GetAllocPageUnit(num_sample) * PAGE_SIZE);
		printf("pages per vertex: %d\n", GetAllocPageUnit(num_sample));
	}

	static size_t GetAllocPageUnit(unsigned int num_sample){
		size_t value_size = ((sizeof(Value) + sizeof(uint64_t) -1 ) / sizeof(uint64_t) ) * sizeof(uint64_t);
		return (sizeof(int) + sizeof(VertexId) + sizeof(unsigned int)*3 + (BaseBitmap::GetArraySize(num_sample) * sizeof(uint64_t)) * (2+NUM_INIT_ALLOCATED) + value_size * NUM_INIT_ALLOCATED + PAGE_SIZE - 1) / PAGE_SIZE;
//        return (sizeof(VertexId) + sizeof(unsigned int)*3 + (BaseBitmap::GetArraySize(num_sample) * sizeof(uint64_t)) * (2+NUM_INIT_ALLOCATED) + value_size * NUM_INIT_ALLOCATED + PAGE_SIZE - 1) / PAGE_SIZE;
	}

	SerializedSimVertex(VertexId vertex_id, unsigned int num_sample, void** ptr_data) {
		alloc_page_unit = GetAllocPageUnit(num_sample);

		assert(ptr_data != NULL);
		this->ptr_data = ptr_data;

		PtrInit(*ptr_data, num_sample);
		this->vertex_id = vertex_id;
		if(vertex_id == ~0U) {
            *ptr_type = 1;
			*ptr_num_allocated_page = alloc_page_unit;
			*ptr_num_sample = num_sample;
			*ptr_vertex_id = vertex_id;
		}

		if(*ptr_vertex_id != ~0U)
			assert(*ptr_vertex_id == vertex_id);

		value_size = ((sizeof(Value) + sizeof(uint64_t) -1 ) / sizeof(uint64_t) ) * sizeof(uint64_t);
		entry_size = sizeof(uint64_t) * BaseBitmap::GetArraySize(*ptr_num_sample) + value_size;
	}

	VertexId GetVertexId(){
		return vertex_id;
	}

	Bitmap GetActivationBitmap(unsigned int idx){
		assert(idx<2);
		return Bitmap((uint64_t*) ptr_activated_sample_bitmap_data[idx], *ptr_num_sample);
	}

	void* GetVertexValueBitmap(){
		return ptr_value_bitmap;
	}

	unsigned int GetNumValue(){
		return *ptr_num_value;
	}

	std::pair<Value*, Bitmap> GetValueBitmapByIndex(unsigned int idx){
		Value* v = (Value*) (((uint8_t*) ptr_value_bitmap) + entry_size * idx);
		uint64_t* bitmap_data = (uint64_t*) (((uint8_t*) v) + value_size);
		Bitmap bitmap(bitmap_data, (size_t) (*ptr_num_sample));
		return {v, bitmap};
	}

	bool VerifyBitmap(){
		Bitmap test_bitmap(*ptr_num_sample);
		test_bitmap.ClearAll();
		for(int i=0; i<*ptr_num_value; i++) {
			auto it = GetValueBitmapByIndex(i);
			test_bitmap.Or(it.second);
		}
		return test_bitmap.IsAllSet();
	}

	std::pair<Value*, Bitmap> Find(Value value){
		for(int i=0; i<*ptr_num_value; i++){
			Value* _ptr_value_bitmap = (Value*)(((uint8_t*) ptr_value_bitmap) + entry_size * i);
			if(memcmp(_ptr_value_bitmap, &value, sizeof(Value))== 0){
				uint64_t* bitmap_data = (uint64_t*) (((uint8_t*) _ptr_value_bitmap) + value_size);
				Bitmap bitmap(bitmap_data, *ptr_num_sample);
				return {_ptr_value_bitmap, bitmap};
			}
		}
		return {NULL, NULL};
	}

	size_t GetCurrentSize(){
		return sizeof(unsigned int) * 4 + sizeof(uint64_t) * BaseBitmap::GetArraySize(*ptr_num_sample)*2+ entry_size * (*ptr_num_value);
	}

	unsigned int GetNumAllocatedPage(){
		return *ptr_num_allocated_page;
	}

	void Expand(){
		unsigned int prev_num_allocated_page = *ptr_num_allocated_page;
		unsigned int num_sample = *ptr_num_sample;
		void* previous_data = *ptr_data;
		unsigned int previous_num_allocated_page = *ptr_num_allocated_page;

		size_t num_allocated_size = (prev_num_allocated_page+alloc_page_unit) * PAGE_SIZE;
		void* tmp;

		assert(*ptr_vertex_id == vertex_id);
		posix_memalign(&tmp, PAGE_SIZE, num_allocated_size);
		memset(tmp, 0x00, num_allocated_size);
		memcpy(tmp, *ptr_data, prev_num_allocated_page * PAGE_SIZE);
		free(*ptr_data);
		PtrInit(tmp, num_sample);
		assert(*ptr_vertex_id == vertex_id);
		*ptr_num_allocated_page = prev_num_allocated_page+alloc_page_unit;//+=  alloc_page_unit;
		assert(num_allocated_size == (*ptr_num_allocated_page*PAGE_SIZE));
		assert(*ptr_data == tmp);
	}

    void TransformSelf(){
        unsigned int num_sample = *ptr_num_sample;
        size_t num_allocated_size = SerializedBundleVertex<Value>::GetAllocPageUnit(num_sample) * PAGE_SIZE;
        void* tmp;
        assert(*ptr_vertex_id == vertex_id);
        posix_memalign(&tmp, PAGE_SIZE, num_allocated_size);
//        printf("%p %d alloc\n", tmp, num_allocated_size);
        TransformCopy(tmp);
        free(*ptr_data);
        *ptr_data = tmp;
        // todo: type_map
    }

    void TransformCopy(void* tmp){
        unsigned int num_sample = *ptr_num_sample;
        SerializedBundleVertex<Value> bundle_vertex(~0U, num_sample, tmp);
        bundle_vertex.SetVertexId(vertex_id);
        bundle_vertex.GetActivationBitmap(0).CopyFrom(
                Bitmap((uint64_t*) ptr_activated_sample_bitmap_data[0], *ptr_num_sample));
        bundle_vertex.GetActivationBitmap(1).CopyFrom(
                Bitmap((uint64_t*) ptr_activated_sample_bitmap_data[1], *ptr_num_sample));
        for(size_t i=0; i<*ptr_num_value; i++) {
            auto value_bitmap = GetValueBitmapByIndex(i);
            Value* ptr_value = value_bitmap.first;
            Bitmap bitmap = value_bitmap.second;

            for(size_t j=0; j<num_sample; j++){
                if(bitmap.Get(j)){
                    Value* ptr_bundle_value = bundle_vertex.GetVertexAttributeByIndex(j);
                    memcpy(ptr_bundle_value, ptr_value, sizeof(Value));
                }
            }
        }
	}

    void Print(){
		for(int i=0; i<GetNumValue(); i++){
			auto tmp = GetValueBitmapByIndex(i);
			printf("%d: %d-%d (%p)\n", GetVertexId(), i, tmp.second.GetSetCount(), tmp.second.GetData());
		}
	}

	void Check(){
//		Bitmap bm(NUM_SAMPLE);
//		bm.ClearAll();
//		Bitmap bm2(NUM_SAMPLE);
//		bm2.ClearAll();
		size_t sum_cnt = 0;
		for(int i=0; i<*ptr_num_value; i++){
			auto it = GetValueBitmapByIndex(i);
			sum_cnt += it.second.GetSetCount();
//			bm2.And(it.second);
//			assert(bm2.IsAllClear());
//			bm.Or(it.second);
//			bm2.CopyFrom(bm);
		}
		if(sum_cnt != NUM_SAMPLE){
			printf("%d %d %d\n", vertex_id, *ptr_num_value, sum_cnt);
		}
//		assert(sum_cnt == NUM_SAMPLE);
//		if(!bm.IsAllSet()){
//			printf("%d:: %d\n", vertex_id, *ptr_num_value);
//			for(int i=0; i<*ptr_num_value; i++){
//				auto it = GetValueBitmapByIndex(i);
//				printf("%d: %d\n", vertex_id, it.second.GetSetCount());
//			}
//		}
//		assert(bm.IsAllSet());
	}

	std::pair<Value*, Bitmap> AllocNewValue(bool* is_expand){
		if(is_expand)
			*is_expand = false;
		for(int i=0; i<*ptr_num_value; i++){
			auto it = GetValueBitmapByIndex(i);
			if(it.second.IsAllClear()){
				return {it.first, it.second};
			}
		}

		size_t current_size = GetCurrentSize() + entry_size;
		if(current_size > *ptr_num_allocated_page * PAGE_SIZE) {
			Expand();
			assert(is_expand);
			*is_expand = true;
		}

		Value* ptr_value = (Value*) (((uint8_t*) ptr_value_bitmap) + entry_size * (*ptr_num_value));
		Bitmap bitmap((uint64_t*) (((uint8_t*) ptr_value) + value_size), *ptr_num_sample);
		bitmap.ClearAll();
		*ptr_num_value += 1;

//		if(*ptr_num_value > NUM_SAMPLE * 2)
//			Check();

		assert(*ptr_num_value <= NUM_SAMPLE * 2);

		if(*ptr_num_value > STAT_MAX_NUM_VALUE)
			STAT_MAX_NUM_VALUE = *ptr_num_value;

		if(current_size > STAT_MAX_VERTEX_SIZE)
			STAT_MAX_VERTEX_SIZE = current_size;
		assert((*ptr_num_value) * entry_size + sizeof(unsigned int) * 4 + BaseBitmap::GetArraySize(*ptr_num_sample)*sizeof(uint64_t)*2 <= (*ptr_num_allocated_page) * PAGE_SIZE);
		return {ptr_value, bitmap};
	}

	void DeleteValue(){

	}
};

#endif //SAGE_SERIALIZED_VALUE_BITMAP_HPP
