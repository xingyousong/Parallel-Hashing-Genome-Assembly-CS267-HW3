import re 
from functools import reduce
#fetching all data from the output
filename = [1,2,4,8,16,32,64]
middle = []
nodes = []
contigs = []
starter_nodes = []

#for store
maxlist = []
minlist = []
avglist = []
node_num_list = []
contigs_num_list = []

for x in range(0,len(filename)):
	#for each of the file compute the min, max,mean
	#print('insane_'+str(filename[x]))
	with open('insane_'+str(filename[x]), 'r') as inF:
		for line in inF:
			#compute the total time
			raw_str = re.search("(\d+).(\d+) total",line)
			total_time = re.search("(\d+).(\d+)",raw_str.group())
			middle.append(float(total_time.group()))

			#compute the problem size
			#node numbers
			node_str = re.search("(\d+) nodes",line)
			node_num = re.search("(\d+)",node_str.group())
			nodes.append(int(node_num.group()))
			#contigs
			contig_str = re.search("(\d+) contigs",line)
			contig_num = re.search("(\d+)",contig_str.group())
			contigs.append(int(contig_num.group()))

		#get the 3 different value for strong scaling
		#reset the list
		#reduction work
	inF.close()
	avg = reduce(lambda x, y: x + y, middle) / len(middle)
	ma  = max(middle)
	mi  = min(middle)
	nodenum = reduce(lambda x, y:x + y, nodes)
	contigsnum = reduce(lambda x, y:x + y, contigs)
	#refresh
	middle = []
	nodes = []
	contigs =[]


	maxlist.append(ma)
	minlist.append(mi)
	avglist.append(avg)
	node_num_list.append(nodenum)
	contigs_num_list.append(contigsnum)

#print("problem size: in sequence nodes-contigs")
#print(node_num_list)
#print(contigs_num_list)
print("the max value list:")
print(maxlist)
print("the ave value list:")
print(avglist)
print("the min value list:")
print(minlist)

sse_avg = []
sse_min = []
sse_max = []
print("the strong scaling efficiency is(in max-ave-min sequence):")
for x in range(0,len(filename)):
	#compute max
	sse_max.append(maxlist[0]/(maxlist[x]*filename[x]))
	sse_avg.append(avglist[0]/(avglist[x]*filename[x]))
	sse_min.append(minlist[0]/(minlist[x]*filename[x]))
print(sse_max)
print(sse_avg)
print(sse_min)