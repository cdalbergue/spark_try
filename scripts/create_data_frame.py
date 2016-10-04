from pyspark.sql import SQLContext
# import regex
import re
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