# Coding Communication in Distributed Computing for Graph Analysis

## Setting up AWS Instance

1. Create 5 AWS EC2 Ubuntu Instance (make sure all instance have same VPC and subnet)
2. Add security group to allow all inbound and outbound traffic
3. Setup password for 'ubuntu' user on every AWS instance in order to SCP from one instance to another instance.

```bash
$ sudo passwd ubuntu
Enter unix password: default
Reenter unix password: default
```

4. Enable password login by editing sshd_config and replace "PasswordAuthenticationn No" to "PasswordAuthentication Yes"

```bash
sudo vim /etc/ssh/sshd_config
sudo service sshd restart
```


## Exchanging ssh key between master and workers node

1. Login to master node

```bash
ssh ubuntu@instance_ip
```

2. Generate ssh-rsa key
```bash
$ ssh-keygen -t rsa
$ cp ~/.ssh/id_rsa.pub ~/.ssh/authorized_keys
```

3. Copy authorization key from master to every worker node
```bash
$ scp -r ~/.ssh ubuntu@node1_ip:~/
```

4. Repeat previous step for every worker node

## Setting up MCP

1. On your master node, clone this repository

```bash
$ git clone https://github.com/vaibhawraj/coen241_project.git CPR
```

2. Execute install.sh script. This script will install Message Passing Interface and Python Dependency

```bash
$ chmod +x ~/CPR/install.sh
$ ./CPR/install.sh
```

## Executing project script

Sequence of execution has been put together in runScript.py for following input
```
N = [120, 300, 600, 1200, 2400, 4800] # Nodes
R = [2, 3] # Number of redundant instance of Node
K = 4 # Number of workers
```

1. Modify machinefile to include private IPv4 address for master and each worker nodes in following syntax

```bash
$ cd ~/CPR
$ vim machinefile
10.8.0.1 slots=1
10.8.0.2 slots=1
10.8.0.3 slots=1
10.8.0.4 slots=1
10.8.0.5 slots=1
```

2. Copy script "cp.sh" ensures that master and worker nodes are sync in terms of generated graph files. Make it executable as well

```bash
$ chmod +x cp.sh
```

3. Execute runScript.py (this step would take 10-15 minutes)

```bash
$ python3 runScript.py
```

4. Analyze the results
```bash
$ cd ~/CPR/result
$ cat uncoded.csv
N,K,R,tmap,tshuffle,treduce,texecution
120,4,1,0.0004687309265136719,0.011050224304199219,0.00027561187744140625,0.011794567108154297
300,4,1,0.0027017593383789062,0.023016929626464844,0.002035856246948242,0.027754545211791992
600,4,1,0.010357141494750977,0.10412764549255371,0.0076787471771240234,0.12216353416442871
1200,4,1,0.04134559631347656,0.3819997310638428,0.02959418296813965,0.452939510345459
2400,4,1,0.1566760540008545,1.2512645721435547,0.11142182350158691,1.519362449645996
4800,4,1,0.6262199878692627,4.459690570831299,0.5083262920379639,5.594236850738525
```
Similarly for 2_Coded.csv and 3_Coded.csv. Result stores CSV file for every graph node from the above nodeset and execution time for Map Phase (tmap), Reduce Phase (treduce) and Shuffle Phase(tshuffle) alongwith total execution time (texecution)

## Using our instance to test

1. SSH to our master instance
```bash
$ ssh ubuntu@18.191.64.75
default
```
2. Change directory to "CPR_e" and execute runScript.py (this step would take 10-15 minutes)
```bash
$ cd ~/CPR_e
$ python3 runScript.py
```

3. Analyze the results
```bash
$ cd result
$ cat uncoded.csv
N,K,R,tmap,tshuffle,treduce,texecution
120,4,1,0.0004687309265136719,0.011050224304199219,0.00027561187744140625,0.011794567108154297
300,4,1,0.0027017593383789062,0.023016929626464844,0.002035856246948242,0.027754545211791992
600,4,1,0.010357141494750977,0.10412764549255371,0.0076787471771240234,0.12216353416442871
1200,4,1,0.04134559631347656,0.3819997310638428,0.02959418296813965,0.452939510345459
2400,4,1,0.1566760540008545,1.2512645721435547,0.11142182350158691,1.519362449645996
4800,4,1,0.6262199878692627,4.459690570831299,0.5083262920379639,5.594236850738525
```
