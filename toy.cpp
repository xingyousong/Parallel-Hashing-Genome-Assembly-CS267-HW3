#include <cstdio>
#include <cstdlib>
#include <vector>
#include <list>
#include <set>
#include <numeric>
#include <cstddef>
#include <iostream>
#include <upcxx/upcxx.hpp>
#include "kmer_t.hpp"

using namespace std;
using namespace upcxx;

struct Entry{
  kmer_pair k_pair;
  Entry(){}
  Entry(kmer_pair k_pair){
    this->k_pair = k_pair;
  }
  void print(){
    cout << "This kmer is: " <<  this->k_pair.kmer_str() << endl;
  }
};

global_ptr<Entry> create_or_fetch_global_hashMap(const int size){
  global_ptr<Entry> global_hashMap = nullptr;
  if (rank_me() == 0){
    global_hashMap = new_array<Entry>(size);
  }
  global_hashMap = broadcast(global_hashMap, 0).wait();
  return global_hashMap;
}

global_ptr<int> create_or_fetch_global_kmers_mapping(){
  global_ptr<int> global_mapping = nullptr;
  if (rank_me() == 0){
    global_mapping = new_array<int>(rank_n());
  }
  global_mapping = broadcast(global_mapping, 0).wait();
  return global_mapping;
}

void put_my_size_into_globl_kmers_mapping(global_ptr<int> global_mapping, const int mySize){
  global_ptr<int> my_pos = global_mapping + rank_me();
  rput(mySize, my_pos).wait();
}

void get_global_mapping(global_ptr<int> global_mapping_ptr, int* my_local_mapping_ptr){
  upcxx::future<> f = make_future();
  for (int i = 0; i < rank_n(); i++) {
    f = when_all(f, rget(global_mapping_ptr + i)
      .then(
        [=](int itsSize) {
          my_local_mapping_ptr[i] = itsSize;
        }
      )
    );
  }
  f.wait();
}

void test_atomic_fetch_and_add(global_ptr<int> global_mapping_ptr, int myValue){
  for (int i = 0; i < rank_n(); i++){
    for (int j = 0; j < 100; j++){
      atomic_fetch_add(global_mapping_ptr + i, rank_me(), memory_order_relaxed).wait();
    }
  }
}

void test_atomic_get(global_ptr<int> global_mapping_ptr, int* my_local_mapping_ptr){
  upcxx::future<> f = make_future();
  for (int i = 0; i < rank_n(); i++) {
    f = when_all(f, atomic_get(global_mapping_ptr + i, memory_order_relaxed)
      .then(
        [=](int itsSize) {
          my_local_mapping_ptr[i] = itsSize;
        }
      )
    );
  }
  f.wait();
}

int main(int argc, char **argv) {
  upcxx::init();

  global_ptr<int> global_mapping_ptr = create_or_fetch_global_kmers_mapping();
  upcxx::barrier();

  put_my_size_into_globl_kmers_mapping(global_mapping_ptr, rank_me());
  upcxx::barrier();

  test_atomic_fetch_and_add(global_mapping_ptr, rank_me());
  upcxx::barrier();

  int local_global_mapping[rank_n()];
  test_atomic_get(global_mapping_ptr, local_global_mapping);
  upcxx::barrier();

  for (int i = 0; i < rank_n(); i++){
    cout << local_global_mapping[i] << " ";
  }
  cout << endl;


  upcxx::finalize();
  return 0;
}
