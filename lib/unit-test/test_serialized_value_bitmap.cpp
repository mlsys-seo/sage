#include <stdio.h>
#include "serialized_value_bitmap.hpp"

struct V{
	int a;
	int b;
};

int main(){
	int num_sample = 1000000;

#if 1
	SerializedSimVertex<V> value_bitmap(128, num_sample);
	for(int i=0; i<num_sample; i++) {
		std:: cout << i << "\n";
		V v = {i%2, 0};;
		auto ret = value_bitmap.Find(v);
		if(ret.first == NULL) {
			bool alloc_new_page = false;
			auto entry = value_bitmap.AllocNewValue(&alloc_new_page);
			if(alloc_new_page){
				printf("allocated new page (page: %d)\n", value_bitmap.GetNumAllocatedPage());
				//todo: get disk_offset & set vertex_map(offset, size)
			}
			*(entry.first) = v;
			entry.second.Set(i);
		}else{
			ret.second.Set(i);
		}
	}
	printf("size: %d\n", value_bitmap.GetNumAllocatedPage());
#else
	SerializedBundleVertex<V> vertex_array(128, num_sample);
	for(int i=0; i<num_sample; i++) {
		V* v = vertex_array.GetVertexByIndex(i);
		v->a = i;
		v->b = 128;
	}

	for(int i=0; i<num_sample; i++) {
		V* v = vertex_array.GetVertexByIndex(i);
		assert(v->a == i);
		assert(v->b == 128);
	}
	printf("complete\n");
#endif

}