
#-DDEBUG
#	g++ -o linked -O3 -g linked.cpp -lrt 

all: wbtree_slot_bitmap.cpp
	g++ -o wbtree_slot_bitmap -O3 wbtree_slot_bitmap.cpp -lrt 

debug: wbtree_slot_bitmap.cpp
	g++ -o wbtree_slot_bitmap wbtree_slot_bitmap.cpp -lrt -g
