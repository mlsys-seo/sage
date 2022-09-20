#include <iostream>
#include <bitmap.hpp>
#include <assert.h>

int main() {
	const int bitmap_size = 32;
	Bitmap bm1 = Bitmap(bitmap_size);

	for(int i=0; i<bitmap_size; i++)
		assert(bm1.Get(i) == false);

	bool a = bm1.Get(10);
	assert(a == false);

	bm1.Set(10);
	bool b = bm1.Get(10);
	assert(b == true);

	bm1.Clear(10);
	bool c = bm1.Get(10);
	assert(c == false);

	bm1.SetAll();

	for(int i=0; i<bitmap_size; i++)
		assert(bm1.Get(i) == true);

	Bitmap bm2;
	bm2 = bm1;
	bm2.SetAll();

	bm1.ClearAll();

	for(int i=0; i<bitmap_size; i++)
		assert(bm1.Get(i) == false);

	for(int i=0; i<bitmap_size; i++)
		assert(bm2.Get(i) == true);

	bm1.Set(1);
	bm2.Clear(1);

	Bitmap bm3;
	bm3 = bm2 | bm1;
	for(int i=0; i<bitmap_size; i++) {
		assert(bm3.Get(i) == true);
	}

	Bitmap bm4;
	bm4 = bm1 & bm2;

	for(int i=0; i<bitmap_size; i++) {
		assert(bm4.Get(i) == false);
	}

	Bitmap bm5;
	bm5 = bm3 - bm2;
	assert(bm1 == bm5);

	unsigned int cursor = NULL;
	std::vector<off_t> vec = bm2.GetKeys(&cursor, 10);
	for(int i=0; i<vec.size(); i++){
		std::cout << vec[i] << " ";
	}
	std::cout << "\n";
	std::cout << cursor << "\n";

	bm1.Print();
	bm2.Print();
	bm3.Print();
	bm4.Print();
	bm5.Print();

	std::cout << "Success\n";

	return 0;
}