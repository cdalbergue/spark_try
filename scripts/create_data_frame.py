from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
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

	#  laod file
	lines = sc.textFile("../data/users.csv")
	# create the part for each line, colsep="\t" (with regexp)
	parts = lines.map(lambda line: re.split(r'\t', line))

	# map each line
	# UserID,Gender,Age,Occupation,Zip-code
	users = parts.map(lambda array: Row(userid=array[0], gender=array[1], age=int(array[2]), occupation=array[3], zipcode=string(array[4])))

	# map each line
	schemaUser = sqlContext.createDataFrame(users)

	# create a temporary table
	schemaUser.registerTempTable("users")

	# create parquet
	schemaUser.write.parquet("users.parquet")