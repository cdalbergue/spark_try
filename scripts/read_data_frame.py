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

  number_of_movies = sqlContext.sql("SELECT count(*) FROM users").collect()
  print("##############################################################################################################")
  print("################# Number of movies ###########################################################################")
  for line in number_of_movies:
    print(line)

  number_of_movies_rated_by_months = sqlContext.sql("SELECT month('timestamp'), count(*) FROM ratings GROUP BY month('timestamp')")
  print("##############################################################################################################")
  print("################# number_of_movies_rated_by_months ###########################################################################")
  for line in number_of_movies_rated_by_months.collect():
    print(line)
  
  number_of_moves_rated_by_user = sqlContext.sql("SELECT userid, count(*) FROM ratings GROUP BY userid")
  print("##############################################################################################################")
  print("################# number_of_moves_rated_by_user ###########################################################################")
  for line in number_of_moves_rated_by_user.collect():
    print(line)

  number_of_movies_rated_by_user_by_months = sqlContext.sql("SELECT users.userid, month('ratings.timestamp'), count(*) FROM users LEFT JOIN ratings on users.userid = ratings.userid GROUP BY users.userid, month('ratings.timestamp')")
  print("##############################################################################################################")
  print("################# number_of_movies_rated_by_user_by_months ###########################################################################")
  for line in number_of_movies_rated_by_user_by_months.collect():
    print(line)
  
  mean_rating_by_movie = sqlContext.sql("SELECT movieid, mean(rating) FROM ratings GROUP BY movieid")
  print("##############################################################################################################")
  print("################# mean_rating_by_movie ###########################################################################")
  for line in mean_rating_by_movie.collect():
    print(line)

  mean_rating_by_movie_by_gender = sqlContext.sql("SELECT movieid, users.gender, mean(rating) FROM ratings LEFT JOIN users on ratings.userid = users.userid GROUP BY movieid, users.gender")
  print("##############################################################################################################")
  print("################# mean_rating_by_movie ###########################################################################")
  for line in mean_rating_by_movie.collect():
    print(line)

  mean_rating_by_user = sqlContext.sql("SELECT userid, mean(rating) FROM ratings GROUP BY userid")
  print("##############################################################################################################")
  print("################# mean_rating_by_user ###########################################################################")
  for line in mean_rating_by_user.collect():
    print(line)

  mean_rating_by_user_by_months = sqlContext.sql("SELECT userid, mean(rating), month('timestamp') FROM ratings GROUP BY userid, month('timestamp')")
  print("##############################################################################################################")
  print("################# mean_rating_by_user_by_months ###########################################################################")
  for line in mean_rating_by_user_by_months.collect():
    print(line)

  top_ten_voters = sqlContext.sql("SELECT userid, count(rating) AS `nb_votes` FROM ratings GROUP BY userid ORDER BY DESC(nb_votes) LIMIT 10")
  print("##############################################################################################################")
  print("################# top_ten_voters ###########################################################################")
  for line in top_ten_voters.collect():
    print(line)