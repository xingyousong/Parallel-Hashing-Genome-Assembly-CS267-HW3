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
#include <mutex>
#include <atomic>
#include "butil.hpp"
#include "read_kmers.hpp"

using namespace std;
using namespace upcxx;

#define MASTER 0
#define DEBUG 1

struct LockerTable{
  uint64_t size;
  vector<atomic<char>> is_occupied_slots;

  LockerTable(){
  }

  LockerTable(uint64_t size){
    this->size = size;
    is_occupied_slots = vector<atomic<char>>(size);
  }

  pair<bool, uint64_t> check_slot_available(uint64_t slot_pos){
    uint64_t k = slot_pos;
    while (k < size){
      if (!is_occupied_slots[k].fetch_or(1)){
        return make_pair(true, k);
      }
      k++;
    }
    return make_pair(false, slot_pos);
  }
};

// Read k-mers from fname.
// If nprocs and rank are given, each rank will read
// an appropriately sized block portion of the k-mers.
std::vector <kmer_pair> my_read_kmers(const std::string &fname, int nprocs = 1, int rank = 0) {
  size_t num_lines = line_count(fname);
  size_t split = num_lines / nprocs;
  size_t start = split*rank;
  size_t size = 0;
  if (rank == rank_n() - 1)
    size = num_lines - start;
  else
    size = split;


  FILE *f = fopen(fname.c_str(), "r");
  if (f == NULL) {
    throw std::runtime_error("read_kmers: could not open " + fname);
  }
  const size_t line_len = KMER_LEN + 4;
  fseek(f, line_len*start, SEEK_SET);

  std::shared_ptr <char> buf(new char[line_len*size]);
  fread(buf.get(), sizeof(char), line_len*size, f);

  std::vector <kmer_pair> kmers;

  for (size_t line_offset = 0; line_offset < line_len*size; line_offset += line_len) {
    char *kmer_buf = &buf.get()[line_offset];
    char *fb_ext_buf = kmer_buf + KMER_LEN+1;
    kmers.push_back(kmer_pair(std::string(kmer_buf, KMER_LEN), std::string(fb_ext_buf, 2)));
  }
  fclose(f);
  return kmers;
}

//Extract contig
std::string my_extract_contig(const vector<kmer_pair> &contig) {
  std::string contig_buf = "";

  contig_buf += contig.front().kmer_str();

  for (auto it = contig.begin(); it != contig.end(); it++){
    if ((*it).forwardExt() != 'F'){
      contig_buf += (*it).forwardExt();
    }
  }
  return contig_buf;
}
