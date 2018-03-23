#include "hashmap.h"
using namespace std;
using namespace upcxx;

uint64_t hashMapSize;
uint64_t averageOffset; //the average number of slots each process is in charge of
uint64_t mySize; //the number of slots I am in charge of
uint64_t myOffset; //the sum of the number of slots other processes have (who are in front of me)
uint64_t n_kmers;

LockerTable myLockerTable;
uint64_t* local_meta_data;

global_ptr<kmer_pair> global_hashMap;
global_ptr<uint64_t> global_meta_data;
vector<global_ptr<kmer_pair>> global_hashMap_ptrs;


/* -------------------------------- Helper Functions -------------------------------- */
kmer_pair retrieve_from_global_hashMap(uint64_t slot_pos){
  int whichProc = slot_pos / averageOffset;
  return rget(global_hashMap_ptrs[whichProc] + slot_pos % averageOffset).wait();
}

/* -------------------------------- RPC FUNCTIONS -------------------------------- */
//Return the slot position to insert the kmer in the global memory
uint64_t rpc_find_open_slots(uint64_t slot_pos){
  //If find an open slot locally, return the index to the rpc callers
  pair<bool, uint64_t> result = myLockerTable.check_slot_available(slot_pos);
  if (result.first){
    return result.second + myOffset;
  }
  return -1;
}

/* -------------------------------- SELF (FAKE) RPC FUNCTIONS -------------------------------- */
uint64_t self_rpc_find_open_slots(uint64_t slot_pos){
  //If find an open slot locally, return the index to the rpc callers
  pair<bool, uint64_t> result = myLockerTable.check_slot_available(slot_pos);
  if (result.first){
    return result.second + myOffset;
  }
  return -1;
}


/* -------------------------------- Intialize Global Memory --------------------------------*/

//Create a hashmap within the global memory
//Let Master create the hashmap and broadcast the pointers to others
void create_and_fetch_global_hashMap(){
  global_ptr<kmer_pair> my_hashmap = new_array<kmer_pair>(mySize);
  global_ptr<kmer_pair> other_hashmap = nullptr;
  for (int i = 0; i < rank_n(); i++){
    global_hashMap_ptrs[i] = broadcast(my_hashmap, i).wait();
  }
}

//Create a 1d array to store how many slots each process is responsible for
//Let Master create the array and broadcast the pointer to others
global_ptr<uint64_t> create_or_fetch_global_meta_data(){
  global_ptr<uint64_t> global_meta_data = nullptr;
  if (rank_me() == MASTER){
    global_meta_data = new_array<uint64_t>(rank_n());
  }
  global_meta_data = broadcast(global_meta_data, MASTER).wait();
  return global_meta_data;
}

//Put the amount of slots into the global_meta_data so that others can later fetch it
void put_my_size_into_global_meta_data(){
  global_ptr<uint64_t> my_pos = global_meta_data + rank_me();
  rput(mySize, my_pos).wait();
}

//Each process get the meta data from the global_meta_data ptr
//and put into its own local_meta_data array
void fetch_others_meta_data(){
  upcxx::future<> f = make_future();
  for (int i = 0; i < rank_n(); i++) {
    f = when_all(f, rget(global_meta_data + i)
      .then(
        [=](uint64_t itsSize) {
          local_meta_data[i] = itsSize;
        }
      )
    );
  }
  f.wait();
}

/* ------------------------------- HashMap Related Functions -------------------------------*/

//Insert kmer
//Called locally per process
bool insert_kmer(kmer_pair kmer){
  //Calculate the hash first
  uint64_t hash = kmer.hash() % hashMapSize;
  //Find the appropriate slot
  int whichProc = hash / averageOffset;
  uint64_t whichSlot = hash - whichProc * averageOffset;
  //Call RPC
  //TODO: OPTIMIZATION
  uint64_t which_global_hashMap_slot = -1;
  int processes_checked = 0;
  do{
    which_global_hashMap_slot = rpc(whichProc, rpc_find_open_slots, whichSlot).wait();
    processes_checked++;
    whichProc = (whichProc + 1) % rank_n();
    whichSlot = 0;
  }while (which_global_hashMap_slot == -1 && processes_checked <= rank_n());

  if (which_global_hashMap_slot == -1 && processes_checked > rank_n()){
    throw runtime_error("INSERTION FAILED");
  }

  //Insert the kmer globally
  //TODO: OPTIMIZATION
  //DEBUG
  assert(which_global_hashMap_slot < hashMapSize);
  whichProc = which_global_hashMap_slot / averageOffset;
  rput(kmer_pair(kmer), global_hashMap_ptrs[whichProc] + which_global_hashMap_slot % averageOffset).wait();
  return true;
}

//Find kmer
//Called locally per process
pair<bool, kmer_pair> find_kmer(const pkmer_t& key_kmer){
  //Output
  bool hasFound = false;
  kmer_pair val_kmer;
  //Probe
  uint64_t probe = 0;
  //Calculate the hash first
  uint64_t hash = key_kmer.hash();
  do {
    uint64_t slot_pos = (hash + probe++) % hashMapSize;
    kmer_pair kmer_pair_in_that_slot = retrieve_from_global_hashMap(slot_pos);
    if (kmer_pair_in_that_slot.kmer == key_kmer){
      hasFound = true;
      val_kmer = kmer_pair_in_that_slot;
    }
  } while (!hasFound && probe < hashMapSize);


  return make_pair(hasFound, val_kmer);
}

/* ------------------------------- Functions To be Called in Main ------------------------------- */

//Initialize some variables
void initialize_variables(){
  //initialize some vars here
  local_meta_data = (uint64_t*)malloc(rank_n() * sizeof(uint64_t));
  averageOffset = hashMapSize / rank_n();
  myOffset = averageOffset * rank_me();
  mySize = averageOffset;
  myLockerTable = LockerTable(mySize);
  global_hashMap_ptrs = vector<global_ptr<kmer_pair>>(rank_n());
}

//Housekeeping
//Create pointers, set variables
void houseKeeping(std::string kmer_fname){
  n_kmers = line_count(kmer_fname);
  // Load factor of 0.6
  hashMapSize = ceil(n_kmers * (1.0 / 0.6) / rank_n()) * rank_n();

  //Initialize variables
  initialize_variables();
  upcxx::barrier();

  //Set global_hashMap_ptrs
  create_and_fetch_global_hashMap();
  upcxx::barrier();

  //Get global_meta_data pointer
  global_meta_data = create_or_fetch_global_meta_data();
  upcxx::barrier();

  //Put my size into the global_meta_data array
  put_my_size_into_global_meta_data();
  upcxx::barrier();

  //Fetch all the processes' size into the local_meta_data array
  fetch_others_meta_data();
  upcxx::barrier();
}

//Create start_nodes vector & insert kmers into the global_hashMap
void create_global_hashMap_and_start_nodes(vector<kmer_pair>& start_nodes, vector<kmer_pair>& kmer_vocabulary){
  for (auto it = kmer_vocabulary.begin(); it != kmer_vocabulary.end(); it++){
    bool canInsert = insert_kmer(*it);
    if (!canInsert){
      throw std::runtime_error("ERROR: global hashMap insertion failed!");
    }
    if ((*it).backwardExt() == 'F'){
      start_nodes.push_back(*it);
    }
  }
}


int main(int argc, char **argv) {
  upcxx::init();

  if (argc < 2) {
    BUtil::print("usage: srun -N nodes -n ranks ./kmer_hash kmer_file [verbose|test]\n");
    upcxx::finalize();
    exit(1);
  }

  std::string kmer_fname = std::string(argv[1]);
  std::string run_type = "";

  if (argc >= 3) {
    run_type = std::string(argv[2]);
    if (run_type.at(0) == '.'){
      run_type = run_type.substr(1, run_type.length());
    }
  }

  int ks = kmer_size(kmer_fname);
  if (ks != KMER_LEN) {
    throw std::runtime_error("Error: " + kmer_fname + " contains " +
      std::to_string(ks) + "-mers, while this binary is compiled for " +
      std::to_string(KMER_LEN) + "-mers.  Modify packing.hpp and recompile.");
  }

  ////////// STEP 1
  /* --- HouseKeeping --- */
  //Create pointers, set variables, etc.
  houseKeeping(kmer_fname);

  if (run_type == "verbose"){
    BUtil::print("Process %d: hashMapSize %d, averageOffset %d, myOffset %d, mySize %d, myLockerTable size: %d \n",
      rank_me(), hashMapSize, averageOffset, myOffset, mySize, myLockerTable.size);
  }

  if (run_type == "verbose"){
    for (int i = 0; i < rank_n(); i++){
      BUtil::print("Process %d: Process %d is in charge of %d slots \n", rank_me(), i, local_meta_data[i]);
    }
  }

  ////////// STEP 2
  /* --- Read kmers from the files --- */
  //Each process reads a portion of it
  vector<kmer_pair> kmer_vocabulary = my_read_kmers(kmer_fname, upcxx::rank_n(), upcxx::rank_me());
  if (run_type == "verbose") {
    BUtil::print("Process %d: initializing hash table of size %d for %d kmers.\n",
      rank_me(), mySize, kmer_vocabulary.size());
  }

  ////////// STEP 3
  /* --- Insert start nodes into the start_nodes vector and global hashMap --- */
  auto start = std::chrono::high_resolution_clock::now();
  vector<kmer_pair> start_nodes;
  create_global_hashMap_and_start_nodes(start_nodes, kmer_vocabulary);
  auto end_insert = std::chrono::high_resolution_clock::now();
  upcxx::barrier();

  double insert_time = std::chrono::duration <double> (end_insert - start).count();
  if (run_type == "verbose") {
    BUtil::print("Process %d: finished inserting in %lf\n", rank_me(), insert_time);
    BUtil::print("Process %d: has %d start_nodes \n", rank_me(), start_nodes.size());
  }
  upcxx::barrier();

  ////////// STEP 4
  /* ---- For each start node, find the next kmer based on the global hashMap ---- */
  auto start_read = std::chrono::high_resolution_clock::now();
  vector<vector<kmer_pair>> contigs;
  for (auto thisStartNode = start_nodes.begin(); thisStartNode != start_nodes.end(); thisStartNode++){
    vector<kmer_pair> contig;
    contig.push_back(*thisStartNode);
    while (contig.back().forwardExt() != 'F'){
      pair<bool, kmer_pair> queryResult = find_kmer(contig.back().next_kmer());
      if (!queryResult.first){
        throw runtime_error("ERROR: KMER FIND QUERY FAILED. NO KMER FOUND");
      }
      contig.push_back(queryResult.second);
    }
    contigs.push_back(contig);
  }
  auto end_read = std::chrono::high_resolution_clock::now();
  upcxx::barrier();

  ////////// STEP 5
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration <double> read = end_read - start_read;
  std::chrono::duration <double> insert = end_insert - start;
  std::chrono::duration <double> total = end - start;

  int numKmers = std::accumulate(contigs.begin(), contigs.end(), 0,
    [] (int sum, const vector<kmer_pair> &contig) {
      return sum + contig.size();
    });

  if (run_type != "test") {
    BUtil::print("Assembled in %lf total\n", total.count());
  }

  if (run_type == "verbose") {
    BUtil::print("Rank %d reconstructed %d contigs with %d nodes from %d start nodes."
      " (%lf read, %lf insert, %lf total)\n", upcxx::rank_me(), contigs.size(),
      numKmers, start_nodes.size(), read.count(), insert.count(), total.count());
  }

  if (run_type == "test") {
    std::ofstream fout(run_type + "_" + std::to_string(upcxx::rank_me()) + ".dat");
    for (auto it = contigs.begin(); it != contigs.end(); it++) {
      fout << my_extract_contig(*it) << std::endl;
    }
    fout.close();
  }

  upcxx::finalize();
  return 0;
}
