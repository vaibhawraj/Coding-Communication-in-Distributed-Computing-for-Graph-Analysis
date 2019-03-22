import os


N=[120,300,600,1200,2400,4800,9600,19200]
R=[2,3]
K=4
os.system("rm result/*.csv")
for r in R:
    os.system("echo N,K,R,tmap,tshuffle,treduce,texecution > result/"+str(r)+"_coded.csv")
os.system("echo N,K,R,tmap,tshuffle,treduce,texecution > result/uncoded.csv")
for n in N:
    os.system("rm *.txt")
    print("node",n)
    print("Generating Graph")
    os.system("python3 GraphPartitionCreator.py 4 " + str(n))
    os.system("./cp.sh")
    os.system("mpirun -np 5 -machinefile machinefile -mca btl_tcp_if_include tun0 -mca btl_base_warn_component_unused 0 python3 uncodedPageRank.py 4 " + str(n) + " >> result/uncoded.csv")
    for r in R:
        os.system("echo python3 GraphPartitionCreatorCoded.py 4 " + str(n) + " " + str(r))
        os.system("python3 GraphPartitionCreatorCoded.py 4 " + str(n) + " " + str(r))
        os.system("echo ./cp.sh")
        os.system("./cp.sh")
	os.system("clear")
        os.system("echo mpirun -np 5 -machinefile machinefile -mca btl_tcp_if_include tun0 -mca btl_base_warn_component_unused 0 python3 codedPageRank.py 4 " + str(n) + " " + str(r))
        os.system("mpirun -np 5 -machinefile machinefile -mca btl_tcp_if_include tun0 -mca btl_base_warn_component_unused 0 python3 codedPageRank.py 4 " + str(n) + " " + str(r) + " >> result/" +str(r) + "_" + "coded.csv")
        os.system("cat result/" + str(r) + "_" + "coded.csv")
