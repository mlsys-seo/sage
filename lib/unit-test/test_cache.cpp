#include <iostream>
#include "cache.hpp"
#include <cstdio>
#include <cstdlib>

#define PAGE_SIZE 4096

void make_test_file(){
	const int num_pages = 1024;
	char buf[PAGE_SIZE] = {0};

	FILE* fp = fopen("test.bin", "w+");
	for(int i=0; i<num_pages; i++){
		sprintf(buf, "%d", i);
		fwrite(buf, PAGE_SIZE, 1, fp);
//		printf("%s\n", buf);
	}
	fclose(fp);
}

int main() {
	make_test_file();

	//typedef char Page[PAGE_SIZE];
	typedef void* Page;
	Cache c(10,PAGE_SIZE);

	int ret;
	FILE *fp = fopen("test.bin", "r");

	for(int i=0; i<5; i++) {
		Page* p_cached = c.Get(i, &ret);
		if(ret == CACHE_MISS) {
			fseek(fp, i * PAGE_SIZE, SEEK_SET);
			fread(*p_cached, PAGE_SIZE, 1, fp);
			*((int*)(*p_cached)) = i;
			c.Put(i, p_cached);
		}
		c.SetPriority(i, 0);
		assert(*((int*)*p_cached) == i);
	}

	for(int i=0; i<10; i++) {
		Page* p_cached = c.Get(i, &ret);

		if(ret == CACHE_MISS) {
			fseek(fp, i * PAGE_SIZE, SEEK_SET);
			fread(*p_cached, PAGE_SIZE, 1, fp);
			*((int*)(*p_cached)) = i;
			c.Put(i, p_cached);
		}
		c.SetPriority(i, 0);
		assert(*((int*)*p_cached) == i);
	}

	c.SetPriority(1,0);
	c.SetPriority(0,0);


	for(int i=3; i<13; i++) {
		Page* p_cached = c.Get(i, &ret);
		if(ret == CACHE_MISS) {
			fseek(fp, i * PAGE_SIZE, SEEK_SET);
			fread(*p_cached, PAGE_SIZE, 1, fp);
			*((int*)(*p_cached)) = i;
			c.Put(i, p_cached);
		}
		c.SetPriority(i, 0);
		assert(*((int*)*p_cached) == i);
	}

	return 0;
}