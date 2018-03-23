export GASNET_MAX_SEGSIZE=8GB
export UPCXX_SEGMENT_MB=1024
srun -N $1 -n $2 ./kmer_hash /global/project/projectdirs/mp309/cs267-spr2018/hw3-datasets/large.txt test
