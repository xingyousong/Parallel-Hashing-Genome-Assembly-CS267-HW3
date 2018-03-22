from math import*

#hardcode parameters
test_nodes = 4514197
test_contigs = 5736
large_nodes = 27000544
large_contigs = 210550
insane_nodes = 89710742
insane_contigs = 860329
#time
insane_1_32 = 15.600129687499997
large_1_32 = 4.761927875000001
test_1_32 = 0.74528071875

#time
insane_2_64 = 23.227071539682544
large_2_64 = 6.207574796875
test_2_64 = 0.8650062734374999

#add whatever you want to compute here

#computation
#slope [i] = ( log(t[i+1]) - log(t[i]) ) / ( log(n[i+1]*1.0) - log(n[i]*1.0) );
slope_1_32_nodes_1 = (log(large_1_32) - log(test_1_32))/(log(large_nodes)-log(test_nodes))
slope_1_32_nodes_2 = (log(insane_1_32) - log(large_1_32))/(log(insane_nodes)-log(large_nodes))
slope_2_64_nodes_1 = (log(large_2_64) - log(test_2_64))/(log(large_nodes)-log(test_nodes))
slope_2_64_nodes_2 = (log(insane_2_64) - log(large_2_64))/(log(insane_nodes)-log(large_nodes))
print(slope_1_32_nodes_2)
print(slope_1_32_nodes_1)
print(slope_2_64_nodes_2)
print(slope_2_64_nodes_1)
