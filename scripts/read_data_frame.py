#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext, Row
# import regex
import re

if __name__ == '__main__':
  # conf est l'objet permettant de setter tous les paramètre d'execution de spark
  conf = SparkConf()
  # par exemple le nom de l'application
  conf.setAppName("tp2 esiea")
  # maintenant on va définir un CONTEXT spark nous permettant de signifier à python qu'on execute spark map reduce
  sc= SparkContext(conf = conf)
  # create sql context
  sqlContext = SQLContext(sc)

  # Read in the Parquet file created above. Parquet files are self-describing so the schema is preserved.
  # The result of loading a parquet file is also a DataFrame.
  parquetFile = sqlContext.read.parquet("users.parquet")
  print("##############################################################################################################")
  print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
  parquetFile.registerTempTable("temp_movies");
  number_of_movies = sqlContext.sql("SELECT count(*) AS `count` FROM temp_movies")
  number_of_movies = number_of_movies.map(lambda p: "count: " + p.count)
  number_of_movies.printSchema()
  print(number_of_movies)

