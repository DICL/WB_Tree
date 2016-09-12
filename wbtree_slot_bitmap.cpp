// TODO : delete all try & catch
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
#define PAGESIZE 128
#include <bitset>
#else
//  #define PAGESIZE 8192
//  #define PAGESIZE 4096
//  #define PAGESIZE 2048
#define PAGESIZE 1024
//  #define PAGESIZE 512
//  #define PAGESIZE 256
#endif

#define CACHE_LINE_SIZE 64 

#define LEAF 1
#define INTERNAL 0

enum MergeTypes {
  kNone = 0,
  kUpdate = 1,
  kEmpty = 2,
  kShift = 3,
  kMerge = 4,
};

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

// class MergeInfo {
// // MergeTypes
// //  kNone   : Nothing to do.
// //  kUpdate : Update key to the new_key.
// //  kEmpty  : Delete the parent's key.
// //  kShift  : Shift one key from neighbor node.
// //  kMerge  : Merge with the neighbor node.
//  public:
//   MergeInfo() {
//   }
//   MergeInfo(MergeTypes type, MergeTypes nbr_type) {
//     type_ = type;
//     nbr_type_ = nbr_type;
//   }
//   MergeInfo(MergeTypes type, MergeTypes nbr_type, page *nbr, int64_t new_key,
//             int64_t nbr_new_key, bool is_left) {
//     type_ = type;
//     nbr_type_ = nbr_type;
//     nbr_ = nbr;
//     new_key_ = new_key;
//     nbr_new_key_ = nbr_new_key;
//     is_nbr_left_ = is_left;
//   }
//   inline void set(MergeTypes type) { type_ = type; }
//   inline void set(MergeTypes type, int64_t new_key) {
//     type_ = type;
//     new_key_ = new_key;
//   }
//   inline void set_nbr(MergeTypes type) { nbr_type_ = type; }
//   inline void set_nbr(MergeTypes type, int64_t new_key) {
//     nbr_type_ = type;
//     nbr_new_key_ = new_key;
//   }
//   inline MergeTypes get_type() { return type_; }
//   inline MergeTypes get_nbr_type() { return nbr_type_; }
//   inline int64_t get_new_key() { return new_key_; }
//   inline int64_t get_nbr_new_key() { return nbr_new_key_; }
//   inline page* get_nbr() { return nbr_; }
//   inline bool is_nbr_left() { return is_nbr_left_; }
// 
//  private:
//   MergeTypes type_;
//   MergeTypes nbr_type_;
//   int64_t new_key_;
//   int64_t nbr_new_key_;
//   page* nbr_;
//   bool is_nbr_left_;
// 
//  friend class page;
// };

class header{ // 27 bytes
  private:
    uint8_t cnt; // 1 bytes
    uint8_t flag; // 1 byte, leaf or internal
    uint8_t fragmented_bytes; // 1 byte
    page* leftmost_ptr; // 8 bytes
    int64_t split_key; // 8 bytes
    page* sibling_ptr; // 8 bytes
    friend class page;
    friend class btree;
};

class entry{
  private:
    //int16_t len;  // need this for variable-length record and defragmentation.
    int64_t key; // 8 bytes
    char* ptr;   // 8 bytes

    friend class page;
};

const int cardinality = (PAGESIZE-sizeof(header))/ (sizeof(entry)+2);
const uint64_t error = 1UL << 57;

class page{
  private:
    header hdr;  // header in persistent memory
    entry records[cardinality]; // slots in persistent memory

    uint64_t bitmap; // 0 or 1 for validation, rest for entries
    uint8_t slot_array[cardinality];  

  public:
    page(){}

    page(short f)
    {
      bitmap=1;
      hdr.cnt=0;
      hdr.flag=f;
      hdr.fragmented_bytes=0;
      hdr.leftmost_ptr = NULL;  

      hdr.split_key = 0;
      hdr.sibling_ptr = NULL;

    }

    page(page* left, int64_t key, page* right) // this is called when tree grows
    {
      bitmap=1;
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

    inline uint8_t nextSlotOff2()
    {
      uint64_t bit = 2;
      uint8_t i=0;
//      bitset<64> x(bitmap);
//      cout << hdr.cnt <<"/"<< cardinality<< ": " << x << endl;
      while ( bit != 0 && i < cardinality) {
        if ( (bitmap & bit) == 0 ) {
          return i;
        }
        bit = bit << 1;
        ++i;
      }

#ifdef DEBUG
      bitset<64> bm (bitmap);
      cout << hdr.cnt << endl;
      cout << bm << endl;
#endif 
      cout << "no space: " << bitmap << endl;
      assert(!"not enough space: This should not happen\n");
    }


    inline int64_t getKey(int i)
    {
      return (records[slot_array[i]]).key;
    }

    inline char* getPtr(int i)
    {
      return (records[slot_array[i]]).ptr;
    }

    inline page* getLeftPtr(int i) {
      if (i > 0) {
        return (page*)records[slot_array[i - 1]].ptr;
      } else if (i == 0) {
        return hdr.leftmost_ptr;
      } else {
        return NULL;
      }
    }

    inline page* getLeftPtr(int i, page *left_parent) {
      if (i == -1 && left_parent != NULL) {
        return left_parent->getRightMostPtr();
      } else {
        return getLeftPtr(i);
      }
    }

    inline page* getRightPtr(int i) {
      if (i < hdr.cnt - 1) {
        return (page*)records[slot_array[i + 1]].ptr;
      } else {
        return NULL;
      }
    }

    inline page* getRightPtr(int i, page *right_parent) {
      if (i == hdr.cnt && right_parent != NULL) {
        return right_parent->getLeftMostPtr();
      } else {
        return getRightPtr(i);
      }
    }

    inline page* getLeftMostPtr() {
      return hdr.leftmost_ptr;
    }

    inline page* getRightMostPtr() {
      return (page*)hdr.records[slot_array[hdr.cnt - 1]].ptr;
    }

    inline entry* getLeftMostEntry() {
      return (entry*)&records[slot_array[0]];
    }

    inline entry* getRightMostEntry() {
      return (entry*)&records[slot_array[hdr.cnt - 1]];
    }

    inline entry* getEntry(int i)
    {
      return (entry*) &records[slot_array[i]];
    }

    split_info* store(int64_t key, char* ptr, int flush ) 
    {
      return store(NULL, key, ptr, flush );
    }

    split_info* store(entry *target_entry, int flush) {
      return store(NULL, target_entry->key, target_entry->ptr, flush);
    }

    split_info* store(char* left, int64_t key, char* right, int flush ) 
    {
#ifdef DEBUG
printf("\n-----------------------\nBEFORE STORE %d\n", key);
print();
#endif

      if ( (bitmap & 1) == 0 ) {
        // TODO: recovery
        bitmap += 1; 
        if(flush)
          clflush((char*) &bitmap, 1);
      }
      if( hdr.cnt < cardinality ){
        // have space
        register uint8_t slot_off = (uint8_t) nextSlotOff2();
        bitmap -= 1;
        if(flush) 
          clflush((char*) &bitmap, 1);

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

          uint64_t bit = (1UL << (slot_off+1));
          bitmap |= bit;
          bitmap+=1;

          if(flush)
            clflush((char*) &bitmap, 8);

          hdr.cnt++; 
          assert(bitmap < error);
        }
        else{
          int pos=0;
          for(pos=0;pos<hdr.cnt;pos++){
            if(key < records[slot_array[pos]].key ){
              break;
            }
          }

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

          uint64_t bit = (1UL << (slot_off+1));
#ifdef DEBUG
          uint64_t backup_bitmap = bitmap;
#endif
          bitmap |= bit;
          bitmap+=1;
//          bitset<64> bm(bitmap);
//          cout << hdr.cnt+1 << ":\t" << bm << endl;

          if(flush)
            clflush((char*) &bitmap, 8);
          hdr.cnt++;
          assert (bitmap < error);
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
        //page* lsibling = new page(hdr.flag); 
//        bitset<64> map(bitmap);
//        cout << hdr.cnt << ":\t" << map << endl;
        page* rsibling = new page(hdr.flag); 
        register int m = (int) ceil(hdr.cnt/2);
        uint64_t bitmap_change = 0;
        //int n = 0;

        bitmap -= 1;
        if(flush) 
          clflush((char*) &bitmap, 8);

        // TODO: redo logging?
        // Maybe I can... 
        //split_info s((page*)this, (uint64_t) records[slot_array[m]].key, (page*) rsibling);
        split_info *s = new split_info((page*)this, (uint64_t) records[slot_array[m]].key, (page*) rsibling);

        if(hdr.flag==LEAF){
          //migrate i > hdr.cnt/2 to a rsibling;
          for(int i=m;i<hdr.cnt;i++){
            rsibling->store(records[slot_array[i]].key, records[slot_array[i]].ptr, 0);
            uint64_t bit = (1UL << (slot_array[i]+1));
            bitmap_change += bit;
//            n++;
          }
        }
        else{
          //migrate i > hdr.cnt/2 to a rsibling;
          rsibling->hdr.leftmost_ptr = (page*) records[slot_array[m]].ptr;
          for(int i=m;i<hdr.cnt;i++){
            rsibling->store(records[slot_array[i]].key, records[slot_array[i]].ptr, 0);
            uint64_t bit = (1UL << (slot_array[i]+1));
            bitmap_change += bit;
//            n++;
          }
        }
        assert ((bitmap & bitmap_change) == bitmap_change);
        bitmap -= bitmap_change;
        bitmap += 1;
        this->hdr.cnt = m;

        if(flush){
          clflush((char*) this, sizeof(page));
          clflush((char*) rsibling, sizeof(page));
        }
        // split is done.. now let's insert a new entry

        if(key < s->split_key){
          store(left, key, right, 1);
        }
        else {
          rsibling->store(left, key, right, 1);
        }
#ifdef DEBUG
printf("Split done\n");
printf("Split key=%lld\n", s->split_key);
printf("LEFT\n");
lsibling->print();
printf("RIGHT\n");
rsibling->print();
#endif
        return s;
      }
      return NULL;
    }

//     MergeInfo* get_merge_info(page *left_nbr, page *right_nbr) {
//       bool left_not_null = (left_nbr != NULL);
//       bool right_not_null = (right_nbr != NULL);
//       bool both_not_null = (left_not_null && right_not_null);
//       // check if merge needed.
//       if (hdr.cnt < cardinality / 2) {
//         // node under utilized.
//         if ((both_not_null && left_nbr->hdr.cnt > cardinality / 2 &&
//             left_nbr->hdr.cnt >= right_nbr->hdr.cnt) ||
//             (left_not_null && left_nbr->hdr.cnt > cardinality / 2)) {
//           // shift one key from left.
//           entry *tmp_entry = left_nbr->getRightMostEntry();
//           store(tmp_entry, 1);
//           left_nbr->release(left_nbr->hdr.cnt - 1, NULL, NULL, 1);
//           return new MergeInfo(kUpdate, kNone, left_nbr, getKey(0), 0, true);
//         } else if ((both_not_null && right_nbr->hdr.cnt > cardinality / 2 &&
//                    right_nbr->hdr.cnt >= left_nbr->hdr.cnt) ||
//                    (right_not_null && right_nbr->hdr.cnt > cardinality / 2)) {
//           // shift one key from right.
//           entry *tmp_entry = right_nbr->getLeftMostEntry();
//           store(tmp_entry, 1);
//           right_nbr->release(right_nbr->hdr.cnt - 1, NULL, NULL, 1);
//           return new MergeInfo(kNone, kUpdate, right_nbr, 0,
//                                right_nbr->getKey(0), false);
//         } else if ((both_not_null && left_nbr->hdr.cnt <= cardinality / 2 &&
//                    left_nbr->hdr.cnt <= right_nbr->hdr.cnt) ||
//                    (left_not_null && left_nbr->hdr.cnt <= cardinality / 2)) {
//           // merge to left.
//           while (hdr.cnt > 0) {
//             entry *tmp_entry = getLeftMostEntry();
//             left_nbr->store(tmp_entry, 1);
//             release(0, NULL, NULL, 1);
//           }
//           return new MergeInfo(kEmpty, kNone, left_nbr, 0, 0, true);
//         } else if ((both_not_null && right_nbr->hdr.cnt <= cardinality / 2 &&
//                    right_nbr->hdr.cnt <= left_nbr->hdr.cnt) ||
//                    (right_not_null && right_nbr->hdr.cnt <= cardinality / 2)) {
//           // merge to right.
//           while (hdr.cnt > 0) {
//             entry *tmp_entry = getLeftMostEntry();
//             right_nbr->store(tmp_entry, 1);
//             release(0, NULL, NULL, 1);
//           }
//           return new MergeInfo(kEmpty, kUpdate, right_nbr, 0,
//                                right_nbr->getKey(0), false);
//         } else {
//           // No neighbor node
//           if (hdr.cnt > 0) {
//             // nothing to do. terminate.
//             return NULL;
//           } else {
//             // hdr.cnt == 0
//             if (hdr.flag == LEAF) {
//               return new MergeInfo(kEmpty, kNone);
//             } else {
//               // only LEAF can be empty.
//               // INTERNAL nodes should be merged before get empty.
//               // nothing to do.
//               return NULL;
//             }
//           }
//         }
//       } else {
//         // node well utilized.
//         if (pos == 0) {
//           // first key changed.
//           return new MergeInfo(kUpdate, kNone, NULL, getKey(0), 0, 0);
//         } else {
//           // nothing to do.
//           return NULL;
//         }
//       }
//     }

    int update_key(int64_t key, int64_t new_key, int flush) {
      // TODO: Make it to reuse the postition value gathered from path search.
      int pos = hdr.cnt;
      for (pos = 0; pos < hdr.cnt; ++pos) {
        if (records[slot_array[pos]].key == key) {
          break;
        }
      }
      return update_key(pos, new_key, flush);
    }

    int update_key(int pos, int64_t new_key, int flush) {
      if (pos >= 0 && pos < hdr.cnt) {
        if (bitmap & 1UL != 1UL) {
          // TODO: recovery
          bitmap |= 1UL;
          if (flush) {
            clflush((char*)&bitmap, sizeof(int64_t));
          }
        }
        // invalidate the page.
        bitmap &= 0xFFFFFFFFFFFFFFFE;
        if (flush) {
          clflush((char*)&bitmap, sizeof(int64_t));
        }
        // update the key.
        entry *tmp_entry = &records[slot_array[pos]];
        tmp_entry->key = new_key;
        if (flush) {
          clflush((char*)tmp_entry, sizeof(entry));
        }
        // validate the page
        bitmap |= 1UL;
        if (flush) {
          clflush((char*)&bitmap, sizeof(int64_t));
        }
        return pos;
      } else {
#ifdef DEBUG
        cout << "update: Not found ***, " <<
                ((hdr.flag == LEAF)?"LEAF":"INTERNAL") << endl;
        print();
        exit(1);
#endif
        return -1;
      }
    }

    MergeInfo* release(const int64_t &key, const int &flush) {
      return release(key, NULL, NULL, flush);
    }

    MergeInfo* release(const int &pos, const int &flush) {
      return release(pos, NULL, NULL, flush);
    }

    MergeInfo* release(const int64_t &key, page *left_nbr, page *right_nbr,
                       const int &flush) {
      int pos = hdr.cnt;
      for (pos = 0; pos < hdr.cnt; ++pos) {
        if (records[slot_array[pos]].key == key) break;
      }
      return release(pos, left_nbr, right_nbr, flush);
    }

    MergeInfo* release(const int &pos, page *left_nbr, page *right_nbr,
                       const int &flush) {
      if (pos >= 0 && pos < hdr.cnt) {
        // calculate the entry bit.
        int64_t bit = (1UL << (slot_array[pos] + 1));
        if (bitmap & 1UL != 1UL) {
          // TODO: recovery
          bitmap |= 1UL;
          if (flush) {
            clflush((char*)&bitmap, sizeof(int64_t));
          }
        }
        // invalidate the page.
        bitmap &= 0xFFFFFFFFFFFFFFFE;
        if (flush) {
          clflush((char*)&bitmap, sizeof(int64_t));
        }
        // update slot array
        for (int i = pos; i < hdr.cnt - 1; ++i) {
          slot_array[i] = slot_array[i + 1];
        }
        if (flush) {
          clflush((char*)slot_array, sizeof(uint16_t) * (hdr.cnt - 1));
        }
        // update header count
        --hdr.cnt;
        // update the bitmap.
        bitmap ^= bit;
        // validate the page
        bitmap |= 1UL;
        if (flush) {
          clflush((char*)&bitmap, sizeof(int64_t));
        }
        return get_merge_info(left_nbr, right_nbr);
      } else {
        // invalid pos
#ifdef DEBUG
        cout << "release: Not found ***, " <<
                ((hdr.flag == LEAF)?"LEAF":"INTERNAL") << endl;
        print();
        exit(1);
#endif
        return NULL;
      }
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
#ifdef DEBUG
        print();
        exit(1);
#endif
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
        // visit rightmost pointer 
        return getPtr(hdr.cnt-1); // return rightmost ptr
      }
    }

    char* linear_search(int64_t key, int &pos) {
      pos = -1;
      if (hdr.flag == LEAF) {
        for(int i = 0; i < hdr.cnt; ++i) {
          if (records[slot_array[i]].key == key) {
            //printf("found: %lld\n", key);
            pos = i;
            return records[slot_array[i]].ptr;
          }
        }
        return NULL;
#ifdef DEBUG
        printf("Not found************************************\n");
        print();
        exit(1);
#endif
      } else {
        if (key < getKey(0)){
          return (char*) hdr.leftmost_ptr;
        }
        for(int i = 1; i < hdr.cnt; ++i) {
          if (key < records[slot_array[i]].key) {
            // visit child i
            pos = i - 1;
            return records[slot_array[i - 1]].ptr;
          }
        }
        // visit rightmost pointer 
        pos = hdr.cnt - 1;
        return getPtr(hdr.cnt - 1); // return rightmost ptr
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

  public:
    btree(){
      root = new page(LEAF);
      height = 1;
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
      vector<page*> path;
      path.push_back(p);
      while(p){
        if(p->hdr.flag!=LEAF){
          p = (page*) p->linear_search(key);
          //p = (page*) p->binary_search(key);
          path.push_back(p);
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
        assert(right!=NULL);
          split_info *s = p->store(left, key, right, 1); // store
          p = NULL;
          if(s!=NULL){
          // split occurred
          // logging needed
          page *logPage = new page[2];
          memcpy(logPage, s->left, sizeof(page));
          memcpy(logPage+1, s->right, sizeof(page));
          clflush((char*) logPage, 2*sizeof(page));
          // we need log frame header, but let's just skip it for now... need to fix it for later..

          page* overflown = p;
          p = (page*) path.back();
          path.pop_back();
          if(path.empty()){
            // tree height grows here
            page* new_root = new page(s->left, s->split_key, s->right);
            root = new_root;
#ifdef DEBUG
printf("tree grows: root = %x\n", root);
root->print();
#endif 
            //delete overflown;

            delete s;
            break;
          }
          else{
            // this part needs logging 
            p = (page*) path.back();
            left = (char*) s->left;
            key = s->split_key; 
            right = (char*) s->right;
            assert(right!=NULL);

            //delete overflown;
          }

          delete s;
        }
      } while(p!=NULL);
    }

    // UpdateInfo* btree_delete_rebalance(const int64_t &key, page *lnbr, page *p,
    //                                    page *rnbr) {
    //   int pos;
    //   page *child_node = (page*)p->linear_search(key, pos);
    //   page *left_child = p->getLeftPtr(pos, left_nbr);
    //   page *right_child = p->getRightPtr(pos, right_nbr);
    //   if (p->hdr.flag != LEAF) {
    //   
    //   } else {
    //   }
    // }

    // MergeInfo* btree_delete_rebalance(const int64_t &key, page *left_nbr,
    //                                   page *p, page *right_nbr) {
    //   int pos;
    //   page *child_node = (page*)p->linear_search(key, pos);
    //   page *left_child = p->getLeftPtr(pos, left_nbr);
    //   page *right_child = p->getRightPtr(pos, right_nbr);
    //   MergeInfo *merge_info = NULL;
    //   if (p->hdr.flag != LEAF) {
    //     // continue search until leaf node.
    //     if (child_node != NULL) {
    //       merge_info = btree_delete_rebalance(key, left_child, child_node,
    //                                           right_child);
    //     } else {
    //       // this must not happen.
    //       return NULL;
    //     }
    //   } else {
    //     // LEAF node.
    //     if (pos != -1) {
    //       // key found. release the key from the child node.
    //       return p->release(pos, left_nbr, right_nbr, 1);
    //     } else {
    //       // key not found. terminate.
    //       return NULL;
    //     }
    //   }

    //   if (merge_info != NULL) {
    //     // rebalance needed.
    //     // MergeTypes
    //     //  kNone   : Nothing to do.
    //     //  kUpdate : Update the parent's key to new_key.
    //     //  kEmpty  : Delete the parent's key.
    //     page *tmp_nbr;
    //     if (merge_info->is_nbr_left()) {
    //       tmp_nbr = left_nbr;
    //     } else {
    //       tmp_nbr = right_nbr;
    //     }
    //     MergeInfo *ret = new MergeInfo();
    //     // update neighbor
    //     if (merge_info->get_nbr_type() == kNone) {
    //       ret->set_nbr(kNone);
    //     } else if (merge_info->get_nbr_type() == kUpdate) {
    //       if (tmp_nbr->update_key(nbr_pos, merge_info->get_nbr_new_key(), 1)
    //           == 0) {
    //         ret->set_nbr(kUpdate, tmp_nbr->getKey(0));
    //       } else {
    //         ret->set_nbr(kNone);
    //       }
    //     }
    //     // from mid_child
    //     if (merge_info->get_type() == kNone) {
    //       ret->set(kNone);
    //     } else if (merge_info->get_type() == kUpdate) {
    //       if (p->update_key(pos, merge_info->get_new_key(), 1) == 0) {
    //         ret->set(kUpdate, p->getKey(0));
    //       } else {
    //         ret->set(kNone);
    //       }
    //     } else if (merge_info->get_type() == kEmpty) {
    //       // child must be LEAF.
    //       // delete key from p and return 
    //       delete child_node;
    //       MergeInfo *tmp_ret = p->release(pos, left_nbr, right_nbr, 1);
    //     }
    //   } else {
    //     // NULL
    //     // nothing to rebalance. terminate.
    //     delete ret;
    //     return NULL;
    //   }
    // }

    void btree_delete(int64_t key) {
      page *p = root;

      page* path[height];
      int path_pos[height];
      int top = 0;
      path[top++] = p;

      // vector<page*> path;
      // path.push_back(p);
      while (p) {
        if (p->hdr.flag != LEAF) {
          p = (page*)p->linear_search(key, path_pos[top]);
          path[top++] = p;
          // path.push_back(p);
        } else {
          break;
        }
      }
      if (p == NULL) {
        printf("p is NULL!\n");
      }
      int updated_pos = p->release(key, 1);
      // TODO: logging needed?
      while (p != root) {
        page *child = p;
        p = path[--top];
        // p = (page*)path.back();
        // path.pop_back();
        if (p->hdr.flag == LEAF) {
          // Why this happen??
          break;
        }
        if (child != p->hdr.leftmost_ptr) {
          if (child->hdr.cnt == 0) {
            break;
            if (child->hdr.flag == LEAF) {
              // case A: child is empty leaf and not leftmost
              // updated_pos = p->release(key, 1);
              // delete child;
            }
          } else if (updated_pos == 0) {
            // break;
            // case B: child is alive, nonleftmost, first key changed.
            int64_t new_key = child->getKey(0);
            // updated_pos = p->update_key(key, new_key, 1);
            updated_pos = p->update_key_pos(path_pos[top], new_key, 1);
          } else {
            // case C: child is alive, nonleftmost, nonfisrt key changed.
            // nothing to do.
            break;
          }
        } else {
          // case D: child is leftmost.
          // nothing to do.
          break;
        }
      }
    }

    bool btree_check(page *p) {
      if (p->hdr.flag == LEAF) {
        return true;
      }
      for (int i = 0; i < p->hdr.cnt; ++i) {
        if (((page*)p->getPtr(i))->getKey(0) != p->getKey(i)) {
          cout << "parent " << i << "th " << "key= " << p->getKey(i) << endl;
          cout << "child first key= " << ((page*)p->getPtr(i))->getKey(0) <<
                  endl;
          return false;
        }
        return btree_check((page*)p->getPtr(i));
      }
    }

    bool btree_ck() {
      return btree_check(root);
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

  assert(cardinality < 64);

  long *keys;
  unsigned long *values;
  keys = new long[(sizeof(long)*numData)];
  values= new unsigned long[(sizeof(unsigned long)*numData)];

  ifstream ifs;
  ifs.open("../input_1b.txt");

  assert(ifs);

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

  if (!bt.btree_ck()) return 0;


  clflush_cnt = 0;
  clock_gettime(CLOCK_MONOTONIC,&start);
  for(int i=0;i<numData;i++){
    bt.btree_delete(keys[i]);
  }
  clock_gettime(CLOCK_MONOTONIC,&end);

  elapsedTime = (end.tv_sec-start.tv_sec)*1000000000 + (end.tv_nsec-start.tv_nsec);
  cout<<"DELETETION"<<endl;
  cout<<"elapsedTime : "<<elapsedTime/1000 << "usec" <<endl;
  cout<<"clflush_cnt : "<<clflush_cnt<<endl;



  // for(int i=0;i<numData;i++){
  //   bt.btree_insert(keys[i], (char*) keys[i]);
  // }
  // bt.printAll();
  // for(int i=0;i<numData;i++){
  //   bt.btree_delete(keys[i]);
  // }
  // bt.printAll();
}
