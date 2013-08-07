README for Kmeans Clustering Application
========================================

Pre-Condition:
--------------
Assume you have already copy the Twister-Kmeans-${Release}.jar into "apps" directory.
Assume that you have already created a sub directory named "kmeans" inside the data_dir you specified in $TWISTER_HOME/bin/twister.properties file. You can use ./twister.sh mkdir command to create any sub directory. gen_data.sh and run_kmeans.sh scripts are available in the bin directory.

Generating Data:
----------------
./gen_data.sh  [init clusters file][num of clusters][vector length][sub dir][data file prefix][number of files to generate][number of data points]

e.g. ./gen_data.sh init_clusters.txt 2 3 /kmeans km_data 80 80000

Here "sub dir" refers to the directory where you want the data files to be saved remotely. This is a sub directory under data_dir of all the nodes.


Create Partition File:
----------------------
Irrespective of whether you generate data using above method or manually you need to create a partition file to run the application. Please run the following script in $TWISTER_HOM/bin directory as follows.

./create_partition_file.sh [common directory][file filter][partition file]

e.g. ./create_partition_file.sh /kmeans km_data kmeans.pf

Run Kmeans Clustering:
----------------------

Once the above steps are successful you can simply run the following shell script to run Kmeans clustering appliction.

./run_kmeans.sh [init clusters file][number of map tasks][partition file]

e.g. ./run_kmeans.sh init_clusters.txt 80 kmeans.pf

After a while you should be able to see the identified cluster centers.




