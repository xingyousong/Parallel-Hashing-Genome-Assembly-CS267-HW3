export GASNET_MAX_SEGSIZE=32GB
export UPCXX_SEGMENT_MB=4048
srun -N 1 -n $1 ./hashmap /global/project/projectdirs/mp309/cs267-spr2018/hw3-datasets/human-chr14-synthetic.txt test
