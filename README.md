# Airport Flight Data Analytics
1) Run EC2 PC cluster in your system using SSH
2) Install HADOOP and OOZIE
3) Start the HDFS services 
    /start-all.sh
4) Make directories in HDFS
    hadoop fs -mkdir /airport/
    hadoop fs -mkdir /airport/input/
    hadoop fs -mkdir /airport/output/
5) Add input files    
    hadoop fs -put ~/*.csv.bz2 /airport/input/
6) Run OOZIE program
    oozie job -oozie http://172.32.57.188:11000/oozie -config job.properties -run


