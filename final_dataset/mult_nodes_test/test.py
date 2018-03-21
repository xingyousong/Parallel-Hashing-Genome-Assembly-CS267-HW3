import re 
from functools import reduce
#fetching all data from the output
filename = ['1_32','2_64','4_128','8_256']
middle = []
maxlist = []
minlist = []
avglist = []

for x in range(0,len(filename)):
	#for each of the file compute the min, max,mean
	#print('insane_'+str(filename[x]))
	with open('test_'+str(filename[x]), 'r') as inF:
		for line in inF:
		
			a = re.search("(\d+).(\d+) total",line)
			b = re.search("(\d+).(\d+)",a.group())
			middle.append(float(b.group()))

		#get the 3 different value for strong scaling
		#reset the list
		#reduction work
	inF.close()
	avg = reduce(lambda x, y: x + y, middle) / len(middle)
	ma  = max(middle)
	mi  = min(middle)
	middle = []

	maxlist.append(ma)
	minlist.append(mi)
	avglist.append(avg)
print("the max value list:\n")
print(maxlist)
print("the ave value list:\n")
print(avglist)
print("the min value list:\n")
print(minlist)

sse_avg = []
sse_min = []
sse_max = []
print("the strong scaling efficiency is(in max-ave-min sequence):")
for x in range(0,len(filename)):
	#compute max
	sse_max.append(maxlist[0]/(maxlist[x]*2**x))
	sse_avg.append(avglist[0]/(avglist[x]*2**x))
	sse_min.append(minlist[0]/(minlist[x]*2**x))
print(sse_max)
print(sse_avg)
print(sse_min)