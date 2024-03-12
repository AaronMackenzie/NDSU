#Location of Spark directory(M1 Mac only)
cd /opt/homebrew/Cellar/apache-spark/3.5.0/libexec

#Change the number of worker nodes
#Use this before starting the worker
export SPARK_MASTER_HOST=localhost                                                     
export SPARK_MASTER_PORT=7077
export SPARK_WORKER_INSTANCES=6

#Acess NDSU Spark cluster
ssh aaron.misquith@spark.cs.ndsu.edu

#Spark submit command(NDSU CS department cluster URL)
spark-submit --master spark://192.168.0.121:7077 HW.py

#Command to start Spark master in the terminal in a particular port(7077)
sbin/start-master.sh --host localhost --port 7077 

#URL of spark master(7077)
http://localhost:8080

#Command to stop Spark master in the terminal in a particular port(7077)
sbin/stop-master.sh --host localhost --port 7077

#Command to start Spark worker in the terminal in a particular port(7077)
sbin/start-worker.sh spark://localhost:7077

#Command to stop Spark worker in the terminal in a particular port(7077)
sbin/stop-worker.sh spark://localhost:7077

#hdfs command to save the file into hdfs and then check it
hdfs dfs -put text.txt /user/aaron.misquith/
hdfs dfs -ls /user/aaron.misquith/
