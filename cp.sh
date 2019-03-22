#!/bin/bash
  
hosts=$(cut -d' ' -f1 machinefile)
cur_dir=$(pwd)
cd ..
parent_dir=$(pwd)
cd $cur_dir
for host in $hosts;
do
        echo scp -r $cur_dir $host:$parent_dir
        scp -r $cur_dir $host:$parent_dir
done
     
