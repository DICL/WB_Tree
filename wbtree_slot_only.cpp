#include <unistd.h>
#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include <stdlib.h>
#include <math.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <string.h>
#include <cassert>

//#define DEBUG 1

#ifdef DEBUG
//  #define PAGESIZE 128
#else
//  #define PAGESIZE 8192
//  #define PAGESIZE 4096
//  #define PAGESIZE 2048
//  #define PAGESIZE 1024
//  #define PAGESIZE 512
//  #define PAGESIZE 256
#endif

#define CACHE_LINE_SIZE 64 

#define LEAF 1
#define INTERNAL 0


using namespace std;

inline void mfence()
{
  asm volatile("mfence":::"memory");
}

int clflush_cnt = 0;

inline void clflush(char *data, int len)
{
  volatile char *ptr = (char *)((unsigned long)data &~(CACHE_LINE_SIZE-1));

  mfence();
  for(; ptr<data+len; ptr+=CACHE_LINE_SIZE){
    //printf("clflush ptr: %x\n", ptr);
    asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
    clflush_cnt++;
    //printf("clflush cnt: %d\n", clflush_cnt);
  }
  mfence();

}

class btree_log {
  private:
    uint32_t capacity;
    uint32_t size;
    uint32_t prev_size;
    int8_t* log_pg;
    bool commited;
    vector<uint32_t> tx_cnt;

  public:
    btree_log(uint32_t cap) {
      capacity = cap;
      size = 0;
      prev_size = 0;
      log_pg = (int8_t*)malloc(capacity);
      commited = true;
    }
    
    btree_log () {
      capacity = 0;
      size = 0;
      prev_size = 0;
      log_pg = NULL;
      commited = true;
    }

    ~btree_log() {
      delete log_pg;
    }

    void init(uint32_t cap) {
      capacity = cap;
      log_pg = (int8_t*)malloc(capacity);
    }

    void write(int8_t* ptr, uint32_t s) {
     assert ( size + s < capacity );
     memcpy(log_pg + size, ptr, s);
     size += s;
     commited = false;
    }

    void commit() {
      if (size > capacity * 0.7) {
        capacity = 1.5 * capacity;
        int8_t* new_pg = (int8_t*)malloc(capacity);
        memcpy(new_pg, log_pg, size);
        delete log_pg;
        log_pg = new_pg;
      }
      clflush((char*) log_pg + prev_size, size-prev_size);
      tx_cnt.push_back(size);
      prev_size = tx_cnt.back();
      commited = true;
    }

    bool isCommited() {
      return commited;
    }
};

class page;

class split_info{
  public:
    page* left;
    int64_t split_key;
    page* right;
    split_info(page* l, int64_t k, page* r){
      left = l;
      split_key = k;
      right = r;
    }
    friend class page;
};

class header{
  private:
    uint8_t cnt;
    uint8_t flag;
    uint8_t fragmented_bytes;
    page* leftmost_ptr;  
    int64_t split_key;
    page* sibling_ptr;
    friend class page;
    friend class btree;
};

class entry{
  private:
    //int16_t len;  // need this for variable-length record and defragmentation.
    int64_t key;
    char* ptr;

    friend class page;
};

const int cardinality = 8; //slot only 

class page{
  private:
    header hdr;  // header in persistent memory
    entry records[cardinality]; // slots in persistent memory

    uint8_t slot_array[cardinality];  

  public:
    page(){}

    page(short f)
    {
      hdr.cnt=0;
      hdr.flag=f;
      hdr.fragmented_bytes=0;
      hdr.leftmost_ptr = NULL;  

      hdr.split_key = 0;
      hdr.sibling_ptr = NULL;

    }

    page(page* left, int64_t key, page* right) // this is called when tree grows
    {
      hdr.cnt=0;
      hdr.flag=INTERNAL;
      hdr.fragmented_bytes=0;
      hdr.leftmost_ptr = left;  
      hdr.split_key = 0;
      hdr.sibling_ptr = NULL;

      store((char*) left, key, (char*) right, 1);
    }

    char avail_offsets[cardinality];

    inline uint8_t nextSlotOff()
    {
      for(int i=0;i<cardinality;i++){
        avail_offsets[i] = 1;
      }

      for(int i=0;i<hdr.cnt;i++){
        avail_offsets[slot_array[i]] = 0;
      }

      for(int i=0;i<cardinality;i++){
        if(avail_offsets[i] == 1) {
          //printf("available offset=%d\n", i);
          return i;
        }
      }

      printf("not enough space: This should not happen\n");
    }


    inline int64_t getKey(int i)
    {
      return (records[slot_array[i]]).key;
    }

    inline char* getPtr(int i)
    {
      return (records[slot_array[i]]).ptr;
    }

    inline entry* getEntry(int i)
    {
      return (entry*) &records[slot_array[i]];
    }

    split_info* store(int64_t key, char* ptr, int flush ) 
    {
      return store(NULL, key, ptr, flush );
    }

    split_info* store(char* left, int64_t key, char* right, int flush ) 
    {
#ifdef DEBUG
      printf("\n-----------------------\nBEFORE STORE %d\n", key);
      print();
#endif

      if( hdr.cnt < cardinality ){
        // have space
        register uint8_t slot_off = (uint8_t) nextSlotOff();

        if(hdr.cnt==0){  // this page is empty
          entry* new_entry = (entry*) &records[slot_off];
          new_entry->key = (int64_t) key;
          new_entry->ptr = (char*) right;
          if(left!=NULL) {
            hdr.leftmost_ptr = (page*) left;
          }

          if(flush)
            clflush((char*) new_entry, sizeof(entry));

          slot_array[0] = slot_off;
          if(flush)
            clflush((char*) slot_array, sizeof(uint8_t));

          hdr.cnt++; 
        }
        else{
          int pos=0;
          for(pos=0;pos<hdr.cnt;pos++){
            if(key < records[slot_array[pos]].key ){
              break;
            }
          }
          //printf("pos=%d\n", pos);

          if(left!=NULL){
            // this is needed for CoW
            if(pos>0)
              records[slot_array[pos-1]].ptr = (char*) left;
            else
              hdr.leftmost_ptr = (page*) left;
          }
          records[slot_off].key = (int64_t) key;
          records[slot_off].ptr = (char*) right;
          if(flush)
            clflush((char*) &records[slot_off], sizeof(entry));

          for(int i=hdr.cnt-1; i>=pos; i--){
            slot_array[i+1] = slot_array[i];
          }
          slot_array[pos] = slot_off;

          if(flush)
            clflush((char*) slot_array, sizeof(uint8_t)*hdr.cnt);

          hdr.cnt++; 
        }

#ifdef DEBUG
        printf("--------------:\n");
        printf("AFTER STORE:\n");
        print();
        printf("---------------------------------\n");
#endif
      }
      else {
        // overflow
        // TBD: defragmentation not yet implemented.
#ifdef DEBUG
        printf("====OVERFLOW OVERFLOW OVERFLOW====\n");
        print();
#endif

        page* sibling = new page(hdr.flag); 
        register int m = (int) ceil(hdr.cnt/2);

        if(hdr.flag==LEAF){
          //migrate i > hdr.cnt/2 to a sibling;
          for(int i=m;i<hdr.cnt;i++){
            sibling->store(records[slot_array[i]].key, records[slot_array[i]].ptr, 0);
          }

          hdr.split_key = records[slot_array[m]].key;
          hdr.sibling_ptr = sibling;
          hdr.cnt = hdr.cnt - sibling->hdr.cnt;
          clflush((char*) &hdr, sizeof(hdr));
        }
        else{
          //migrate i > hdr.cnt/2 to a sibling;
          for(int i=m+1;i<hdr.cnt;i++){
            sibling->store(records[slot_array[i]].key, records[slot_array[i]].ptr, 0);
          }
          sibling->hdr.leftmost_ptr = (page*) records[slot_array[m]].ptr;
          hdr.split_key = records[slot_array[m]].key;
          hdr.sibling_ptr = sibling;
          hdr.cnt = hdr.cnt - sibling->hdr.cnt-1;
          if(flush)
            clflush((char*) &hdr, sizeof(hdr));
        }
        // split is done.. now let's insert a new entry

        if(key < hdr.split_key){
          store(key, right, 0);
        }
        else {
          sibling->store(key, right, 0);
        }
#ifdef DEBUG
        printf("Split done\n");
        printf("Split key=%lld\n", hdr.split_key);
        printf("LEFT\n");
        print();
        printf("RIGHT\n");
        sibling->print();
#endif
        split_info *s = new split_info((page*)this, (uint64_t) hdr.split_key, (page*) sibling);
        return s;
      }
      return NULL;
    }

    // page::binary_search
    char* binary_search(long long key)
    {
      if(hdr.flag==LEAF){
        register short lb=0, ub=hdr.cnt-1, m;
        while(lb<=ub){
          m = (lb+ub)/2;
          register uint64_t k = records[slot_array[m]].key;
          if( key < k){
            ub = m-1;
          }
          else if( key == k){
            return records[slot_array[m]].ptr;
          }
          else {
            lb = m+1;
          }

        }
        return NULL;
      }
      else {
        register short lb=0, ub=hdr.cnt-1, m;
        while(lb<=ub){
          m = (lb+ub)/2;
          if( key < records[slot_array[m]].key){
            ub = m-1;
          }
          else {
            lb = m+1;
          }
        }
        if(ub>=0) {
          return records[slot_array[ub]].ptr;
        }
        else
          return (char *)hdr.leftmost_ptr;

        return NULL;
      }
    }

    char* linear_search(long long key)
    {
      if(hdr.flag==LEAF){
        for(int i=0;i<hdr.cnt;i++){
          if(records[slot_array[i]].key == key){
            //printf("found: %lld\n", key);
            return records[slot_array[i]].ptr;
          }
        }
        printf("Not found************************************\n");
        //print();
      }
      else {
        if(key < getKey(0)){
          return (char*) hdr.leftmost_ptr;
        }
        for(int i=1;i<hdr.cnt;i++){
          if(key < records[slot_array[i]].key){
            // visit child i
            return records[slot_array[i-1]].ptr;
          }
        }
        // visit rightmost pointer or sibling pointer
        if( hdr.sibling_ptr != NULL && key > hdr.split_key){
          return (char*) hdr.sibling_ptr; // return sibling ptr
        }
        else{
          return getPtr(hdr.cnt-1); // return rightmost ptr
        }
      }
    }

    void print()
    {
      if(hdr.flag==LEAF) printf("leaf\n");
      else printf("internal\n");
      printf("hdr.cnt=%d:\n", hdr.cnt);

      //        printf("slot_array:\n");
      //	  for(int i=0;i<hdr.cnt;i++){
      //              printf("%d ", (uint8_t) slot_array[i]);
      //	  }
      //	  printf("\n");

      for(int i=0;i<hdr.cnt;i++){
        printf("%ld ", getEntry(i)->key);
      }
      printf("\n");

    }

    void printAll()
    {
      if(hdr.flag==LEAF){
        printf("printing leaf node: ");
        print();
      }
      else{
        printf("printing internal node: ");
        print();

        ((page*) hdr.leftmost_ptr)->printAll();
        for(int i=0;i<hdr.cnt;i++){
          ((page*) getEntry(i)->ptr)->printAll();
        }
      }
    }
    friend class btree;
};


class btree{
  private:
    int height;
    page* root;
    btree_log log;

  public:
    btree(){
      root = new page(LEAF);
      height = 1;
      log.init(sizeof(page)*1024);
    }

    // binary search
    void btree_binary_search(long long key){
      page* p = root;
      while(p){
        if(p->hdr.flag==LEAF){
          // found a leaf
          long long d = (long long) p->binary_search(key);
          if(d==0) {
            printf("************* Not Found %lld\n", key);
            p->print();
            exit(1);
          }
          else {
            //printf("Found %lld\n", key);
          }
          return;
        }
        else{
          p = (page*) p->binary_search(key);
        }
      }
      printf("***** Not Found %lld\n", key);
      return;
    }

    void btree_search(long long key){
      page* p = root;
      while(p){
        if(p->hdr.flag!=LEAF){
          p = (page*) p->linear_search(key);
        }
        else{
          // found a leaf
          p->linear_search(key);
          break;
        }
      }
    }

    void btree_insert(long long key, char* right){
      page* p = root;

#ifndef VECTOR
      page* path[height+1];
      int top = 0;
      path[top++] = p;
#else
      vector<page*> path;
      path.push_back(p);
#endif

      while(p){
        if(p->hdr.flag!=LEAF){
          p = (page*) p->linear_search(key);
          //p = (page*) p->binary_search(key);

#ifndef VECTOR
          path[top++] = p;
#else
          path.push_back(p);
#endif
        }
        else{
          // found a leaf p
          break;
        }
      }

      if(p==NULL) 
        printf("something wrong.. p is null\n");

      char *left = NULL;
      do{
        split_info *s = p->store(left, key, right, 1);  // store 
        p = NULL;
        if(s!=NULL){
          // split occurred
          // logging needed
//          page *logPage = new page;
//          memcpy(logPage, s->left, sizeof(page));
          log.write((int8_t*)s->left,sizeof(page));
          //memcpy(logPage+1, s->right, sizeof(page));
//          clflush((char*) logPage, sizeof(page));
          // we need log frame header, but let's just skip it for now... need to fix it for later..

          page* overflown = p;
#ifndef VECTOR
          p = path[--top];
          if(top==0){
#else
            p = (page*) path.back();
            path.pop_back();
            if(path.empty()){
#endif
              // tree height grows here
              page* new_root = new page(s->left, s->split_key, s->right);
              root = new_root;
#ifdef DEBUG
              printf("tree grows: root = %x\n", root);
              root->print();
#endif 

              //                  root->print();
              height++;
              delete s;
              break;
            }
            else{
              //page* parent = (page*) path.back();
              //parent->store(s.split_key, (char*) s.right);
#ifndef VECTOR
              p = path[top-1];
#else
              p = (page*) path.back();
#endif
              key = s->split_key; 
              right = (char*) s->right;
            }

            delete s;
          }
        } while(p!=NULL);
        if (!log.isCommited()) {
          log.commit();
        }
      }

      void printAll(){
        root->printAll();
      }

    };

    int main(int argc,char** argv)
    {
      int dummy=0;
      btree bt;
      struct timespec start, end;

      //    printf("sizeof(page)=%lu\n", sizeof(page));

      if(argc<2) {
        printf("Usage: %s NDATA\n", argv[0]);
      }
      int numData =atoi(argv[1]);

      long *keys;
      unsigned long *values;
      keys = new long[(sizeof(long)*numData)];
      values= new unsigned long[(sizeof(unsigned long)*numData)];

      ifstream ifs;
      ifs.open("../input_1b.txt");

      if(!ifs){
        cout<<"error!"<<endl;
      }

      for(int i=0; i<numData; i++){
#ifdef DEBUG
        keys[i] = rand()%1000;
#else
        ifs >> keys[i]; 
#endif
      }

      ifs.close();

      clflush_cnt=0;
      clock_gettime(CLOCK_MONOTONIC,&start);
      for(int i=0;i<numData;i++){
#ifdef DEBUG
        printf("inserting %lld\n", keys[i]);
#endif
        bt.btree_insert(keys[i], (char*) keys[i]);
#ifdef DEBUG
        bt.btree_search(keys[i]);
#endif
      }
      clock_gettime(CLOCK_MONOTONIC,&end);

      long long elapsedTime = (end.tv_sec-start.tv_sec)*1000000000 + (end.tv_nsec-start.tv_nsec);
      cout<<"INSERTION"<<endl;
      cout<<"elapsedTime : "<<elapsedTime/1000<<endl;
      cout<<"clflush_cnt : "<<clflush_cnt<<endl;

      char *garbage = new char[256*1024*1024];
      for(int i=0;i<256*1024*1024;i++){
        garbage[i] = i;
      }
      for(int i=100;i<256*1024*1024;i++){
        garbage[i] += garbage[i-100];
      }

      clock_gettime(CLOCK_MONOTONIC,&start);
      for(int i=0;i<numData;i++){
        bt.btree_binary_search(keys[i]);
      }
      clock_gettime(CLOCK_MONOTONIC,&end);

      elapsedTime = (end.tv_sec-start.tv_sec)*1000000000 + (end.tv_nsec-start.tv_nsec);
      cout<<"BINARY SEARCH"<<endl;
      cout<<"elapsedTime : "<<elapsedTime/1000 << "usec" <<endl;

      clock_gettime(CLOCK_MONOTONIC,&start);
      for(int i=0;i<numData;i++){
        bt.btree_search(keys[i]);

      }
      clock_gettime(CLOCK_MONOTONIC,&end);

      elapsedTime = (end.tv_sec-start.tv_sec)*1000000000 + (end.tv_nsec-start.tv_nsec);
      cout<<"LINEAR SEARCH"<<endl;
      cout<<"elapsedTime : "<<elapsedTime/1000 << "usec" <<endl;

    }


