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
  parquetFileUsers = sqlContext.read.parquet("users.parquet")
  parquetFileUsers.registerTempTable("users");

  parquetFileMovies = sqlContext.read.parquet("movies.parquet")
  parquetFileMovies.registerTempTable("movies");

  parquetFileRatings = sqlContext.read.parquet("ratings.parquet")
  parquetFileRatings.registerTempTable("ratings");

  query = """select ratings.userid,
        count(*) AS `TOTAL`,
        sum(case rating when 5 then 1 else 0 end) AS `5 STARS`,
        sum(case rating when 4 then 1 else 0 end) AS `4 STARS`,
        sum(case rating when 3 then 1 else 0 end) AS `3 STARS`,
        sum(case rating when 2 then 1 else 0 end) AS `2 STARS`,
        sum(case rating when 1 then 1 else 0 end) AS `1 STARS`,
        mean(rating) AS `MOY STARS`,
        count(rating)/12 AS `moy_months`,
        users.age,
        case gender when 'M' then 0 else 1 end
        from ratings
        left join users on users.userid = ratings.userid
        group by ratings.userid, age, gender"""

  knn_df = sqlContext.sql(query)
  #knn_df = sqlContext.createDataFrame(knn_data)
  knn_df.registerTempTable("knn_data")
  knn_df.write.parquet("knn_data.parquet")