# TP Spark : Wordcount avec HDFS

Compter l'occurence des mots d'un texte de Gutenberg

## Récupérer le chemin du fichier texte
```shell
ubuntu@ip-172-31-xx-xxx:~$ sudo su hdfs

hdfs@ip-172-31-xx-xxx:/home/ubuntu$ hdfs dfs -ls /user/hdfs/gutenberg
Found 4 items
-rw-r--r--   3 ubuntu hdfs    1580890 2018-02-27 11:11 /user/hdfs/gutenberg/4300-0.txt
-rw-r--r--   3 ubuntu hdfs    1428841 2018-02-27 11:11 /user/hdfs/gutenberg/5000-8.txt
drwxr-xr-x   - hdfs   hdfs          0 2018-02-27 14:15 /user/hdfs/gutenberg/gutenberg2-output
-rw-r--r--   3 ubuntu hdfs     674570 2018-02-27 11:11 /user/hdfs/gutenberg/pg20417.txt
```

## Ecrire le programme Spark 

### Charger le fichier depuis Spark
```scala
// TRANSFORMATION : Read data and convert to Dataset
scala> val data = spark.read.textFile("/user/hdfs/gutenberg/4300-0.txt").as[String]

18/03/06 08:28:53 WARN FileStreamSink: Error while looking for metadata directory.
data: org.apache.spark.sql.Dataset[String] = [value: string]
```

### Exécuter un filtre sur le texte
```scala
// TRANSFORMATION : Split word
scala> val words = data.flatMap(value => value.split("\\s+"))
words: org.apache.spark.sql.Dataset[String] = [value: string]

// TRANSFORMATION : Group by lowercased word
scala> val groupedWords = words.groupByKey(_.toLowerCase)
groupedWords: org.apache.spark.sql.KeyValueGroupedDataset[String,String] = org.apache.spark.sql.KeyValueGroupedDataset@3619bc9e

```

### Compter le nombre de records filtrés
```scala
// TRANSFORMATION : Count words
scala> val counts = groupedWords.count()
counts: org.apache.spark.sql.Dataset[(String, Long)] = [value: string, count(1): bigint]

// ACTION : Show Count words
scala> counts.show() 
18/03/05 15:32:20 WARN Executor: Managed memory leak detected; size = 4456448 bytes, TID = 27
+------------+--------+
|       value|count(1)|
+------------+--------+
|      online|       4|
|       those|     312|
|       still|     148|
|    tripping|       5|
|         art|      40|
|         few|      75|
|        some|     305|
|    tortured|       2|
|      slaver|       1|
|       inner|      18|
|      —billy|       1|
|   squealing|       3|
|          c.|      44|
|      deftly|       6|
|     futile:|       1|
|      waters|      18|
| rinderpest.|       1|
|unoffending,|       1|
|      filing|       1|
|        foxy|       4|
+------------+--------+
only showing top 20 rows

// ACTION : Count counts
scala> counts.count
res7: Long = 45098

```

### Ecrire les records filtrés dans un fichier HDFS
```shell
// donner les droits d’accès d’écriture aux dossiers hdfs
ubuntu@ip-172-31-xx-xxx:~/gutenberg$ hdfs dfs -chmod 777 /user/hdfs/gutenberg
```
```scala
// ACTION : saveAsTextFile
scala> counts.rdd.repartition(1).saveAsTextFile("/user/hdfs/gutenberg/output_spark_scala")
```

### Revenir sur HDFS et vérifier la présence des fichiers filtrés
```shell
ubuntu@ip-172-31-24-141:~/gutenberg$ hdfs dfs -ls /user/hdfs/gutenberg
Found 5 items
-rw-r--r--   3 ubuntu hdfs    1580890 2018-02-27 11:11 /user/hdfs/gutenberg/4300-0.txt
-rw-r--r--   3 ubuntu hdfs    1428841 2018-02-27 11:11 /user/hdfs/gutenberg/5000-8.txt
drwxr-xr-x   - hdfs   hdfs          0 2018-02-27 14:15 /user/hdfs/gutenberg/gutenberg2-output
drwxr-xr-x   - spark  hdfs          0 2018-03-05 15:40 /user/hdfs/gutenberg/output_spark_scala
-rw-r--r--   3 ubuntu hdfs     674570 2018-02-27 11:11 /user/hdfs/gutenberg/pg20417.txt
```


