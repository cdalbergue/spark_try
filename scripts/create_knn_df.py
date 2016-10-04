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

  #  laod files
  users = sc.textFile("users.csv")
  ratings = sc.textFile("ratings.csv")
  movies = sc.textFile("movies.csv")

  # create the part for each line, colsep="\t" (with regexp)
  users_parts = users.map(lambda line: re.split(r'\t', line))
  print(users_parts)