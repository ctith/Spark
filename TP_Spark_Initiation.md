# TP Spark Initiation

https://fr.hortonworks.com/tutorial/word-count-sparkr-repl-examples/ 

Deux spark sont installés donc spécifier le spark que l’on va utiliser (la version 2).

Attention à l’utilisateur dans lequel on est loggué : spark utilise un utilisateur spark donc pour télécharger un fichier HDFS depuis Spark, il faut être dans Spark.
```shell
ubuntu@ip-172-31-xx-xxx:~$ sudo su spark
spark@ip-172-31-xx-xxx:/home/ubuntu$ export SPARK_MAJOR_VERSION=2
spark@ip-172-31-xx-xxx:/home/ubuntu$ cd /usr/hdp/current/spark2-client/
spark@ip-172-31-xx-xxx:/usr/hdp/current/spark2-client$ ./bin/spark-shell
```

## SETUP DE LA VERSION DE SPARK  
A TAPER UNIQUEMENT APRES ÊTRE LOGGUÉ SOUS LE USER SPARK
```scala
export SPARK_MAJOR_VERSION=2
```

### VERSION PYTHON

https://hortonworks.com/blog/hands-on-tour-of-apache-spark/ 
http://www.bogotobogo.com/Hadoop/BigData_hadoop_Apache_Spark_PySpark.php 

### VERSION SCALA

https://hortonworks.com/tutorial/a-lap-around-apache-spark/#wordcount-example 

2 points d’entrée sur spark
```
spark-shell pour scala
pi spark pour python 
```

## Pour exécuter le code spark 2.2.0 sur IntelliJ
#### Fichier sbt
```name := "untitled"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0")
```

#### Code
```
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("ClassName").master("local").getOrCreate
val policeInc = spark.read.csv("/path/to/your/csv")
```
