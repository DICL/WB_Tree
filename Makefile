all: bitmap slot

bitmap: wbtree_slot_bitmap.cpp
	g++ -o wbtree_slot_bitmap -O3 wbtree_slot_bitmap.cpp -lrt 

slot: wbtree_slot_only.cpp
	g++ -o wbtree_slot_only -O3 wbtree_slot_only.cpp -lrt 

debug: bitmap_debug slot_debug

bitmap_debug: wbtree_slot_bitmap.cpp
	g++ -o wbtree_slot_bitmap wbtree_slot_bitmap.cpp -lrt -g

slot_debug: wbtree_slot_only.cpp
	g++ -o wbtree_slot_only wbtree_slot_only.cpp -lrt -g

clean:
	rm -rf wbtree_slot_only
	rm -rf wbtree_slot_bitmap
