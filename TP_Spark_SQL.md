# TP Spark avec Scala :gem::gem::gem:

## Fichier Diamonds.csv, a data frame with 53940 rows and 10 variables 
* price price in US dollars (\$326--\$18,823)
* carat weight of the diamond (0.2--5.01)
* cut quality of the cut (Fair, Good, Very Good, Premium, Ideal)
* color diamond colour, from J (worst) to D (best)
* clarity a measurement of how clear the diamond is (I1 (worst), SI2, SI1, VS2, VS1, VVS2, VVS1, IF (best))
* x length in mm (0--10.74)
* y width in mm (0--58.9)
* z depth in mm (0--31.8)
* depth total depth percentage = z / mean(x, y) = 2 * z / (x + y) (43--79)
* table width of top of diamond relative to widest point (43--95)

## Affichage des données téléchargées

### Téléchargement du fichier csv
```
ubuntu:~$ wget "https://raw.githubusercontent.com/intuit/rego/master/examples/diamonds.csv"
```

### Dépôt du fichier csv dans HDFS
```
ubuntu:~$ hdfs dfs -copyFromLocal /home/ubuntu/diamonds.csv /user/hdfs/
```

### Création d'un objet scala pointant vers le dataframe
```scala
ubuntu:~$ sudo su spark
spark:/home/ubuntu$ export SPARK_MAJOR_VERSION=2
spark:/home/ubuntu$ cd /usr/hdp/current/spark2-client/
spark:/usr/hdp/current/spark2-client$ ./bin/spark-shell

scala> val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/hdfs/diamonds.csv")
```

### Vérification du typage du dataframe
```scala
scala> :type df
org.apache.spark.sql.DataFrame
```

### Affichage du schéma inférencé du dataframe
```scala
scala> df.printSchema()
root
 |-- carat: double (nullable = true)
 |-- cut: string (nullable = true)
 |-- color: string (nullable = true)
 |-- clarity: string (nullable = true)
 |-- depth: double (nullable = true)
 |-- table: double (nullable = true)
 |-- price: integer (nullable = true)
 |-- x: double (nullable = true)
 |-- y: double (nullable = true)
 |-- z: double (nullable = true)
```

### Affichage des données
```scala
scala> df.show()
+-----+---------+-----+-------+-----+-----+-----+----+----+----+
|carat|      cut|color|clarity|depth|table|price|   x|   y|   z|
+-----+---------+-----+-------+-----+-----+-----+----+----+----+
| 0.23|    Ideal|    E|    SI2| 61.5|   55|  326|3.95|3.98|2.43|
| 0.21|  Premium|    E|    SI1| 59.8|   61|  326|3.89|3.84|2.31|
| 0.23|     Good|    E|    VS1| 56.9|   65|  327|4.05|4.07|2.31|
| 0.29|  Premium|    I|    VS2| 62.4|   58|  334| 4.2|4.23|2.63|
| 0.31|     Good|    J|    SI2| 63.3|   58|  335|4.34|4.35|2.75|
| 0.24|Very Good|    J|   VVS2| 62.8|   57|  336|3.94|3.96|2.48|
| 0.24|Very Good|    I|   VVS1| 62.3|   57|  336|3.95|3.98|2.47|
| 0.26|Very Good|    H|    SI1| 61.9|   55|  337|4.07|4.11|2.53|
| 0.22|     Fair|    E|    VS2| 65.1|   61|  337|3.87|3.78|2.49|
| 0.23|Very Good|    H|    VS1| 59.4|   61|  338|   4|4.05|2.39|
|  0.3|     Good|    J|    SI1|   64|   55|  339|4.25|4.28|2.73|
| 0.23|    Ideal|    J|    VS1| 62.8|   56|  340|3.93| 3.9|2.46|
| 0.22|  Premium|    F|    SI1| 60.4|   61|  342|3.88|3.84|2.33|
| 0.31|    Ideal|    J|    SI2| 62.2|   54|  344|4.35|4.37|2.71|
|  0.2|  Premium|    E|    SI2| 60.2|   62|  345|3.79|3.75|2.27|
| 0.32|  Premium|    E|     I1| 60.9|   58|  345|4.38|4.42|2.68|
|  0.3|    Ideal|    I|    SI2|   62|   54|  348|4.31|4.34|2.68|
|  0.3|     Good|    J|    SI1| 63.4|   54|  351|4.23|4.29| 2.7|
|  0.3|     Good|    J|    SI1| 63.8|   56|  351|4.23|4.26|2.71|
|  0.3|Very Good|    J|    SI1| 62.7|   59|  351|4.21|4.27|2.66|
+-----+---------+-----+-------+-----+-----+-----+----+----+----+
only showing top 20 rows
```

## Requêtes Spark 

### Prix total des diamants du dataframe
```scala
scala> df.agg(sum("price")).show()
+----------+
|sum(price)|
+----------+
| 212135217|
+----------+
```

### Prix min et max d’un diamant
```scala
scala> df.groupBy("cut").agg(min("price"),max("price"), mean("price"),avg("price")).sort(asc("cut")).show()
+---------+----------+----------+------------------+------------------+
|      cut|min(price)|max(price)|        avg(price)|        avg(price)|
+---------+----------+----------+------------------+------------------+
|     Fair|       337|     18574| 4358.757763975155| 4358.757763975155|
|     Good|       327|     18788| 3928.864451691806| 3928.864451691806|
|    Ideal|       326|     18806| 3457.541970210199| 3457.541970210199|
|  Premium|       326|     18823|4584.2577042999055|4584.2577042999055|
|Very Good|       336|     18818|3981.7598907465654|3981.7598907465654|
+---------+----------+----------+------------------+------------------+
```

### Prix croissant par carat
```scala
scala> df.sort(asc("price")).show()
+-----+---------+-----+-------+-----+-----+-----+----+----+----+
|carat|      cut|color|clarity|depth|table|price|   x|   y|   z|
+-----+---------+-----+-------+-----+-----+-----+----+----+----+
| 0.23|    Ideal|    E|    SI2| 61.5| 55.0|  326|3.95|3.98|2.43|
| 0.21|  Premium|    E|    SI1| 59.8| 61.0|  326|3.89|3.84|2.31|
| 0.23|     Good|    E|    VS1| 56.9| 65.0|  327|4.05|4.07|2.31|
| 0.29|  Premium|    I|    VS2| 62.4| 58.0|  334| 4.2|4.23|2.63|
| 0.31|     Good|    J|    SI2| 63.3| 58.0|  335|4.34|4.35|2.75|
| 0.24|Very Good|    I|   VVS1| 62.3| 57.0|  336|3.95|3.98|2.47|
| 0.24|Very Good|    J|   VVS2| 62.8| 57.0|  336|3.94|3.96|2.48|
| 0.22|     Fair|    E|    VS2| 65.1| 61.0|  337|3.87|3.78|2.49|
| 0.26|Very Good|    H|    SI1| 61.9| 55.0|  337|4.07|4.11|2.53|
| 0.23|Very Good|    H|    VS1| 59.4| 61.0|  338| 4.0|4.05|2.39|
|  0.3|     Good|    J|    SI1| 64.0| 55.0|  339|4.25|4.28|2.73|
| 0.23|    Ideal|    J|    VS1| 62.8| 56.0|  340|3.93| 3.9|2.46|
| 0.22|  Premium|    F|    SI1| 60.4| 61.0|  342|3.88|3.84|2.33|
| 0.31|    Ideal|    J|    SI2| 62.2| 54.0|  344|4.35|4.37|2.71|
|  0.2|  Premium|    E|    SI2| 60.2| 62.0|  345|3.79|3.75|2.27|
| 0.32|  Premium|    E|     I1| 60.9| 58.0|  345|4.38|4.42|2.68|
|  0.3|    Ideal|    I|    SI2| 62.0| 54.0|  348|4.31|4.34|2.68|
|  0.3|     Good|    J|    SI1| 63.4| 54.0|  351|4.23|4.29| 2.7|
|  0.3|     Good|    J|    SI1| 63.8| 56.0|  351|4.23|4.26|2.71|
|  0.3|Very Good|    J|    SI1| 62.7| 59.0|  351|4.21|4.27|2.66|
+-----+---------+-----+-------+-----+-----+-----+----+----+----+
only showing top 20 rows
```

### Afficher les 20 diamants idéals les moins chers
```scala
scala> df.filter(df("cut") === "Ideal").sort(asc("carat")).show(20)
+-----+-----+-----+-------+-----+-----+-----+----+----+----+
|carat|  cut|color|clarity|depth|table|price|   x|   y|   z|
+-----+-----+-----+-------+-----+-----+-----+----+----+----+
| 0.23|Ideal|    E|    SI2| 61.5| 55.0|  326|3.95|3.98|2.43|
| 0.23|Ideal|    J|    VS1| 62.8| 56.0|  340|3.93| 3.9|2.46|
| 0.31|Ideal|    J|    SI2| 62.2| 54.0|  344|4.35|4.37|2.71|
|  0.3|Ideal|    I|    SI2| 62.0| 54.0|  348|4.31|4.34|2.68|
| 0.33|Ideal|    I|    SI2| 61.8| 55.0|  403|4.49|4.51|2.78|
| 0.33|Ideal|    I|    SI2| 61.2| 56.0|  403|4.49| 4.5|2.75|
| 0.33|Ideal|    J|    SI1| 61.1| 56.0|  403|4.49|4.55|2.76|
| 0.23|Ideal|    G|    VS1| 61.9| 54.0|  404|3.93|3.95|2.44|
| 0.32|Ideal|    I|    SI1| 60.9| 55.0|  404|4.45|4.48|2.72|
|  0.3|Ideal|    I|    SI2| 61.0| 59.0|  405| 4.3|4.33|2.63|
| 0.35|Ideal|    I|    VS1| 60.9| 57.0|  552|4.54|4.59|2.78|
|  0.3|Ideal|    D|    SI1| 62.5| 57.0|  552|4.29|4.32|2.69|
|  0.3|Ideal|    D|    SI1| 62.1| 56.0|  552| 4.3|4.33|2.68|
| 0.28|Ideal|    G|   VVS2| 61.4| 56.0|  553|4.19|4.22|2.58|
| 0.32|Ideal|    I|   VVS1| 62.0| 55.3|  553|4.39|4.42|2.73|
| 0.26|Ideal|    E|   VVS2| 62.9| 58.0|  554|4.02|4.06|2.54|
| 0.38|Ideal|    I|    SI2| 61.6| 56.0|  554|4.65|4.67|2.87|
|  0.7|Ideal|    E|    SI1| 62.5| 57.0| 2757| 5.7|5.72|3.57|
|  0.7|Ideal|    G|    VS2| 61.6| 56.0| 2757| 5.7|5.67| 3.5|
| 0.74|Ideal|    G|    SI1| 61.6| 55.0| 2760| 5.8|5.85|3.59|
+-----+-----+-----+-------+-----+-----+-----+----+----+----+
```

### Nombre de diamants du dataframe par clarté de couleur (J= worst, D= best)
```scala
scala> df.groupBy("color").count().sort(desc("color")).show()
+-----+-----+
|color|count|
+-----+-----+
|    J| 2808|
|    I| 5422|
|    H| 8304|
|    G|11292|
|    F| 9542|
|    E| 9797|
|    D| 6775|
+-----+-----+
```

###  Valeur totale des diamants par couleur
```scala
scala> df.groupBy("color").sum("price").sort(asc("sum(price)")).show()
+-----+----------+
|color|sum(price)|
+-----+----------+
|    J|  14949281|
|    D|  21476439|
|    I|  27608146|
|    E|  30142944|
|    F|  35542866|
|    H|  37257301|
|    G|  45158240|
+-----+----------+
```

### Valeur min et max des diamants par couleur
```scala
scala> df.groupBy("color").agg(min("price"), max("price")).sort(desc("color")).show()
+-----+----------+----------+
|color|min(price)|max(price)|
+-----+----------+----------+
|    J|       335|     18710|
|    I|       334|     18823|
|    H|       337|     18803|
|    G|       354|     18818|
|    F|       342|     18791|
|    E|       326|     18731|
|    D|       357|     18693|
+-----+----------+----------+
```

### Prix moyen des diamants par couleur
```scala
scala> df.groupBy("color").avg("price").sort(desc("avg(price)")).show()
+-----+------------------+
|color|        avg(price)|
+-----+------------------+
|    J|  5323.81801994302|
|    I| 5091.874953891553|
|    H| 4486.669195568401|
|    G| 3999.135671271697|
|    F| 3724.886396981765|
|    D|3169.9540959409596|
|    E|3076.7524752475247|
+-----+------------------+
```

### Prix min et max des diamants par carat
```scala
scala> df.groupBy("carat").agg(min("price"),max("price")).sort(asc("carat")).show()
+-----+----------+----------+
|carat|min(price)|max(price)|
+-----+----------+----------+
|  0.2|       345|       367|
| 0.21|       326|       394|
| 0.22|       337|       470|
| 0.23|       326|       688|
| 0.24|       336|       963|
| 0.25|       357|      1186|
| 0.26|       337|       814|
| 0.27|       361|       893|
| 0.28|       360|       828|
| 0.29|       334|      1776|
|  0.3|       339|      2366|
| 0.31|       335|      1917|
| 0.32|       345|      1715|
| 0.33|       366|      1389|
| 0.34|       413|      2346|
| 0.35|       409|      1672|
| 0.36|       410|      1718|
| 0.37|       451|      1764|
| 0.38|       497|      1992|
| 0.39|       451|      1624|
+-----+----------+----------+
only showing top 20 rows
```

### Prix min et max des diamants par coupe
```scala
scala> df.groupBy("cut").agg(min("price"),max("price")).sort(asc("cut")).show()
+---------+----------+----------+
|      cut|min(price)|max(price)|
+---------+----------+----------+
|     Fair|       337|     18574|
|     Good|       327|     18788|
|    Ideal|       326|     18806|
|  Premium|       326|     18823|
|Very Good|       336|     18818|
+---------+----------+----------+
```

### Prix min et max par clarté des diamants (I1 (worst), SI2, SI1, VS2, VS1, VVS2, VVS1, IF (best))
```scala
scala> df.groupBy("clarity").agg(min("price"),max("price")).sort(asc("max(price)")).show()
+-------+----------+----------+
|clarity|min(price)|max(price)|
+-------+----------+----------+
|     I1|       345|     18531|
|   VVS2|       336|     18768|
|   VVS1|       336|     18777|
|    VS1|       327|     18795|
|    SI2|       326|     18804|
|     IF|       369|     18806|
|    SI1|       326|     18818|
|    VS2|       334|     18823|
+-------+----------+----------+
```

### Afficher les diamants supérieurs à 5 carats 
```scala
scala> df.filter(df("carat") > 5).show()
+-----+----+-----+-------+-----+-----+-----+-----+-----+----+
|carat| cut|color|clarity|depth|table|price|    x|    y|   z|
+-----+----+-----+-------+-----+-----+-----+-----+-----+----+
| 5.01|Fair|    J|     I1| 65.5| 59.0|18018|10.74|10.54|6.98|
+-----+----+-----+-------+-----+-----+-----+-----+-----+----+
```

### Afficher les diamants valant entre 800 et 1000 euros
```scala
df.createTempView("diamonds")
scala> spark.sql("SELECT * FROM diamonds WHERE price BETWEEN '800' AND '1000'").show()
+-----+---------+-----+-------+-----+-----+-----+----+----+----+
|carat|      cut|color|clarity|depth|table|price|   x|   y|   z|
+-----+---------+-----+-------+-----+-----+-----+----+----+----+
| 0.41|Very Good|    G|    VS2| 61.4| 57.0|  800|4.76|4.81|2.94|
| 0.32|    Ideal|    F|   VVS2| 62.1| 55.0|  800|4.37|4.39|2.72|
| 0.32|    Ideal|    F|   VVS2| 61.2| 57.0|  800|4.42|4.44|2.71|
| 0.32|    Ideal|    F|   VVS2| 60.9| 56.0|  800|4.44|4.46|2.71|
| 0.32|    Ideal|    F|   VVS2| 61.2| 55.0|  800|4.43|4.46|2.72|
| 0.38|    Ideal|    I|   VVS1| 62.4| 54.8|  800|4.64|4.66| 2.9|
|  0.4|    Ideal|    H|    VS2| 61.6| 57.0|  800|4.71|4.74|2.91|
| 0.36|    Ideal|    D|    VS2| 61.8| 55.0|  800|4.58|4.61|2.84|
| 0.31|    Ideal|    E|    VS1| 62.0| 55.0|  800|4.35|4.38| 2.7|
| 0.31|    Ideal|    E|    VS1| 61.7| 53.0|  800|4.36|4.39| 2.7|
| 0.31|    Ideal|    E|    VS1| 61.3| 55.0|  800| 4.4|4.43| 2.7|
| 0.43|    Ideal|    G|    SI2| 61.1| 55.0|  800|4.92| 4.9| 3.0|
| 0.38|    Ideal|    F|    VS2| 62.0| 56.0|  800|4.63|4.66|2.88|
| 0.38|Very Good|    F|    VS2| 60.9| 56.0|  800|4.67|4.72|2.86|
| 0.38|  Premium|    F|    VS2| 60.5| 59.0|  800|4.67|4.72|2.84|
| 0.38|    Ideal|    G|    VS1| 62.5| 56.0|  800|4.64|4.58|2.88|
| 0.38|Very Good|    F|    VS2| 61.9| 59.0|  800|4.61|4.66|2.87|
| 0.38|    Ideal|    F|    VS2| 62.1| 57.0|  800|4.61|4.67|2.88|
| 0.38|Very Good|    F|    VS2| 62.8| 57.0|  800|4.58|4.62|2.89|
|  0.4|  Premium|    E|    VS1| 61.5| 58.0|  800|4.73|4.76|2.92|
+-----+---------+-----+-------+-----+-----+-----+----+----+----+
only showing top 20 rows
```

## Utilisation de deux dataframes

### df2 : nouvel dataset sans schéma inférencé, comparé à df au schéma inférencé
```scala
scala> val df2 = spark.read.format("csv").option("header", "true").load("/user/hdfs/diamonds.csv")
18/03/06 14:54:16 WARN FileStreamSink: Error while looking for metadata directory.
18/03/06 14:54:16 WARN FileStreamSink: Error while looking for metadata directory.
df2: org.apache.spark.sql.DataFrame = [carat: string, cut: string ... 8 more fields]

scala> df2.printSchema()
root
 |-- carat: string (nullable = true)
 |-- cut: string (nullable = true)
 |-- color: string (nullable = true)
 |-- clarity: string (nullable = true)
 |-- depth: string (nullable = true)
 |-- table: string (nullable = true)
 |-- price: string (nullable = true)
 |-- x: string (nullable = true)
 |-- y: string (nullable = true)
 |-- z: string (nullable = true)
 
scala> df.printSchema()
root
 |-- carat: double (nullable = true)
 |-- cut: string (nullable = true)
 |-- color: string (nullable = true)
 |-- clarity: string (nullable = true)
 |-- depth: double (nullable = true)
 |-- table: double (nullable = true)
 |-- price: integer (nullable = true)
 |-- x: double (nullable = true)
 |-- y: double (nullable = true)
 |-- z: double (nullable = true)

```

### df3 : suppression des colonnes depth, table, x, y, z de df
```scala
scala> val df3 = df.drop($"depth").drop($"table").drop($"x").drop($"y").drop($"z")
df3: org.apache.spark.sql.DataFrame = [carat: double, cut: string ... 3 more fields]

scala> df3.show()
+-----+---------+-----+-------+-----+
|carat|      cut|color|clarity|price|
+-----+---------+-----+-------+-----+
| 0.23|    Ideal|    E|    SI2|  326|
| 0.21|  Premium|    E|    SI1|  326|
| 0.23|     Good|    E|    VS1|  327|
| 0.29|  Premium|    I|    VS2|  334|
| 0.31|     Good|    J|    SI2|  335|
| 0.24|Very Good|    J|   VVS2|  336|
| 0.24|Very Good|    I|   VVS1|  336|
| 0.26|Very Good|    H|    SI1|  337|
| 0.22|     Fair|    E|    VS2|  337|
| 0.23|Very Good|    H|    VS1|  338|
|  0.3|     Good|    J|    SI1|  339|
| 0.23|    Ideal|    J|    VS1|  340|
| 0.22|  Premium|    F|    SI1|  342|
| 0.31|    Ideal|    J|    SI2|  344|
|  0.2|  Premium|    E|    SI2|  345|
| 0.32|  Premium|    E|     I1|  345|
|  0.3|    Ideal|    I|    SI2|  348|
|  0.3|     Good|    J|    SI1|  351|
|  0.3|     Good|    J|    SI1|  351|
|  0.3|Very Good|    J|    SI1|  351|
+-----+---------+-----+-------+-----+
only showing top 20 rows
```

### df4 : dataframe avec uniquement les colonnes carat et price
```scala
scala> val df4 = df3.drop($"cut").drop($"color").drop($"clarity")
df4: org.apache.spark.sql.DataFrame = [carat: double, price: int]

scala> df4.show()
+-----+-----+
|carat|price|
+-----+-----+
| 0.23|  326|
| 0.21|  326|
| 0.23|  327|
| 0.29|  334|
| 0.31|  335|
| 0.24|  336|
| 0.24|  336|
| 0.26|  337|
| 0.22|  337|
| 0.23|  338|
|  0.3|  339|
| 0.23|  340|
| 0.22|  342|
| 0.31|  344|
|  0.2|  345|
| 0.32|  345|
|  0.3|  348|
|  0.3|  351|
|  0.3|  351|
|  0.3|  351|
+-----+-----+
only showing top 20 rows
```

### dfJoin : join de df3 et df4 grâce à la colonne carat et price
```scala
scala> val dfJoin = df4.join(df3, Seq("carat", "price"))
dfJoin: org.apache.spark.sql.DataFrame = [carat: double, price: int ... 3 more fields]

scala> dfJoin.printSchema()
root
 |-- carat: double (nullable = true)
 |-- price: integer (nullable = true)
 |-- cut: string (nullable = true)
 |-- color: string (nullable = true)
 |-- clarity: string (nullable = true)
```
