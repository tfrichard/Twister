#
# Software License, Version 1.0
#
#  Copyright 2003 The Trustees of Indiana University.  All rights reserved.
#
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1) All redistributions of source code must retain the above copyright notice,
#  the list of authors in the original source code, this list of conditions and
#  the disclaimer listed in this license;
# 2) All redistributions in binary form must reproduce the above copyright
#  notice, this list of conditions and the disclaimer listed in this license in
#  the documentation and/or other materials provided with the distribution;
# 3) Any documentation included with all redistributions must include the
#  following acknowledgement:
#
# "This product includes software developed by the Community Grids Lab. For
#  further information contact the Community Grids Lab at
#  http://communitygrids.iu.edu/."
#
#  Alternatively, this acknowledgement may appear in the software itself, and
#  wherever such third-party acknowledgments normally appear.
#
# 4) The name Indiana University or Community Grids Lab or Twister,
#  shall not be used to endorse or promote products derived from this software
#  without prior written permission from Indiana University.  For written
#  permission, please contact the Advanced Research and Technology Institute
#  ("ARTI") at 351 West 10th Street, Indianapolis, Indiana 46202.
# 5) Products derived from this software may not be called Twister,
#  nor may Indiana University or Community Grids Lab or Twister appear
#  in their name, without prior written permission of ARTI.
#
#
#  Indiana University provides no reassurances that the source code provided
#  does not infringe the patent or any other intellectual property rights of
#  any other entity.  Indiana University disclaims any liability to any
#  recipient for claims brought by any other entity based on infringement of
#  intellectual property rights or otherwise.
#
# LICENSEE UNDERSTANDS THAT SOFTWARE IS PROVIDED "AS IS" FOR WHICH NO
# WARRANTIES AS TO CAPABILITIES OR ACCURACY ARE MADE. INDIANA UNIVERSITY GIVES
# NO WARRANTIES AND MAKES NO REPRESENTATION THAT SOFTWARE IS FREE OF
# INFRINGEMENT OF THIRD PARTY PATENT, COPYRIGHT, OR OTHER PROPRIETARY RIGHTS.
# INDIANA UNIVERSITY MAKES NO WARRANTIES THAT SOFTWARE IS FREE FROM "BUGS",
# "VIRUSES", "TROJAN HORSES", "TRAP DOORS", "WORMS", OR OTHER HARMFUL CODE.
# LICENSEE ASSUMES THE ENTIRE RISK AS TO THE PERFORMANCE OF SOFTWARE AND/OR
# ASSOCIATED MATERIALS, AND TO THE PERFORMANCE AND VALIDITY OF INFORMATION
# GENERATED USING SOFTWARE.
#

#!/bin/bash

if [ $# -lt 1 ]; then
    echo Usage: command [parameters]
    echo Supported commands: 
    echo "initdir - Create a directory in all compute nodes (let's call this "data" directory)."
    echo "mkdir   - Create a sub directory inside "data" directory in all compute nodes."
    echo "rmdir   - Remove a sub directory inside "data" directory in all compute nodes."
    echo "put     - Distribute input data across compute nodes."
    echo "putall  - Copy data/resources across compute nodes Sequentially."
    echo "putallp - Copy data/resources across compute nodes parallel."
    echo "putx    - Distribute input data across compute nodes and create partition file."
    echo "putallx - Copy data/resources across compute nodes Sequentially and create partition file."
    echo "putallpx- Copy data/resources across compute nodes parallel and create partition file."
    echo "get     - Get output data to one place."
    echo "cpj     - Copy application jar files to all compute nodes."
    echo "ls      - List files/directories inside the 'data_dir'. "
    exit -1
fi

#Setting local classpath.
cp=$TWISTER_HOME/bin
for i in ${TWISTER_HOME}/lib/*.jar;
  do cp=$i:${cp}
done

case $1 in 'initdir')

       if [ $# -ne 2 ]; then
           echo Usage: initdir [Directory to create - complete path to the directory]
           exit -1
       fi

       for line in `cat $TWISTER_HOME/bin/nodes`;do
                 ssh $line mkdir $2
                 echo $line:$2 created.
       done
       for line in `cat $TWISTER_HOME/bin/driver_node`;do
                 ssh $line mkdir $2
                 echo $line:$2 created.
       done
       ;;


'ls')

if [ $# -gt 4  ]; then
     echo Usage: [ls with no params | ls  \'-a\' to list all | ls subdir | ls subdir \'-a\' ]
     exit -1	
fi

datadir=`sed '/^\#/d' twister.properties | grep 'data_dir' | tail -n 1 | sed 's/^.*=//;s/^[[:space:]]*//;s/[[:space:]]*$//'`

if [ $# = 1 ]; then
     for line in `cat $TWISTER_HOME/bin/nodes`;do
            echo ============ Node: $line ==============
            echo ls $datadir
            ssh $line ls $datadir
            break
     done
     exit 0
fi

if [ $# -eq 2 -a $2 != "-a" ]; then
   for line in `cat $TWISTER_HOME/bin/nodes`;do
            echo ============ Node: $line ==============
            echo ls $datadir/$2
            ssh $line ls $datadir/$2
            break
     done
     exit 0 
fi

if [ $# -eq 2 -a $2 = "-a" ]; then
     for line in `cat $TWISTER_HOME/bin/nodes`;do
            echo ============ Node: $line ==============
            echo ls $datadir
            ssh $line ls $datadir
     done  
     exit 0
fi

if [ $# -eq 3 -a $3 = "-a" ]; then
     for line in `cat $TWISTER_HOME/bin/nodes`;do
            echo ============ Node: $line ==============
            echo ls $datadir/$2
            ssh $line ls $datadir/$2
     done
     exit 0
fi

  ;;

'mkdir')

       if [ $# -ne 2 ]; then
           echo Usage: mkdir [sub directory to create - relative to data_dir specified in twister.properties]
           exit -1
       fi 
  
       datadir=`sed '/^\#/d' twister.properties | grep 'data_dir' | tail -n 1 | sed 's/^.*=//;s/^[[:space:]]*//;s/[[:space:]]*$//'`
  
       for line in `cat $TWISTER_HOME/bin/nodes`;do
                 ssh $line mkdir $datadir/$2
                 echo $line:$datadir/$2 created.
       done
   ;;
  
'rmdir')
  
       if [ $# -ne 2 ]; then
           echo Usage: rmdir [sub directory to delete - - relative to data_dir specified in twister.properties]
           exit -1
       fi

       datadir=`sed '/^\#/d' twister.properties | grep 'data_dir' | tail -n 1 | sed 's/^.*=//;s/^[[:space:]]*//;s/[[:space:]]*$//'`

       for line in `cat $TWISTER_HOME/bin/nodes`;do
           ssh $line rm -rf $datadir/$2
           echo $line:$datadir/$2 deleted.
       done
  ;;

'put')
       if [ $# -gt 6 -o $# -lt 5 ]; then
           echo Usage: put [input data directory \(local\)][destination directory \(remote\)][file filter][num threads][num replications \(optional\)]
           echo destination directory - relative to data_dir specified in twister.properties
           exit -1
       fi
       
       java -Xmx2000m -Xms512m -XX:SurvivorRatio=18 -XX:+UseParallelGC -XX:-UseParallelOldGC -XX:+AggressiveOpts -classpath $cp cgl.imr.script.FileDistributor $2 $3 $4 $5 $6
  ;;

'putall')
       if [ $# -ne 4 ]; then
           echo Usage: putall [input data directory \(local\)][destination directory \(remote\)][file filter]
           echo destination directory - relative to data_dir specified in twister.properties
           exit -1
       fi
      
       datadir=`sed '/^\#/d' twister.properties | grep 'data_dir' | tail -n 1 | sed 's/^.*=//;s/^[[:space:]]*//;s/[[:space:]]*$//'`

       for line in `cat $TWISTER_HOME/bin/nodes`;do
           scp -r $2/$4* $line:$datadir/$3
           echo copied to $line:$datadir/$3
       done
;;

'putallp')
       if [ $# -ne 5 ]; then
           echo Usage: putallp [input data directory \(local\)][destination directory \(remote\)][file filter][num threads]
           echo destination directory - relative to data_dir specified in twister.properties
           exit -1
       fi
       java -Xmx2000m -Xms512m -XX:SurvivorRatio=10 -classpath $cp cgl.imr.script.FileAllDistributor $2 $3 $4 $5
;;

'putx')
       if [ $# -gt 7 -o $# -lt 6 ]; then
           echo Usage: putx [input data directory \(local\)][destination directory \(remote\)][file filter][partition file][num threads][num replications \(optional\)]
           echo destination directory - relative to data_dir specified in twister.properties
           exit -1
       fi
       
       java -Xmx2000m -Xms512m -XX:SurvivorRatio=10 -classpath $cp cgl.imr.script.FileDistributor $2 $3 $4 $6 $7
       create_partition_file.sh $3 $4 $5
  ;;

'putallx')
       if [ $# -ne 5 ]; then
           echo Usage: putallx [input data directory \(local\)][destination directory \(remote\)][file filter][partition file]
           echo destination directory - relative to data_dir specified in twister.properties
           exit -1
       fi
      
       datadir=`sed '/^\#/d' twister.properties | grep 'data_dir' | tail -n 1 | sed 's/^.*=//;s/^[[:space:]]*//;s/[[:space:]]*$//'`

       for line in `cat $TWISTER_HOME/bin/nodes`;do
           scp -r $2/$4* $line:$datadir/$3
           echo copied to $line:$datadir/$3
       done
       create_partition_file.sh $3 $4 $5
;;

'putallpx')
       if [ $# -ne 6 ]; then
           echo Usage: putallpx [input data directory \(local\)][destination directory \(remote\)][file filter][partition file][num threads]
           echo destination directory - relative to data_dir specified in twister.properties
           exit -1
       fi
       java -Xmx2000m -Xms512m -XX:SurvivorRatio=10 -classpath $cp cgl.imr.script.FileAllDistributor $2 $3 $4 $6
       create_partition_file.sh $3 $4 $5
;;

'get')
       if [ $# -ne 4 ]; then
           echo  Usage: get [input directory \(remote\)][file name pattern][destination \(local\)]
           echo  input directory - relative to data_dir specified in twister.properties
           exit -1
       fi
       java -Xmx2000m -Xms512m -XX:SurvivorRatio=10 -classpath $cp cgl.imr.script.FileCollector $2 $3 $4
  ;;

'cpj')

	if [ $# -ne 2 ]; then
    	    echo 1>&2 Usage: [resource to copy to the apps directory]
            exit -1
        fi

        appdir=`sed '/^\#/d' twister.properties | grep 'app_dir' | tail -n 1 | sed 's/^.*=//;s/^[[:space:]]*//;s/[[:space:]]*$//'`

        for line in `cat $TWISTER_HOME/bin/nodes`;do
            #echo $line
            scp $2 $line:$appdir
        done
;;
esac
