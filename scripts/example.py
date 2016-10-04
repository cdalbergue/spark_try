from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
lines = sc.textFile("examples/src/main/resources/people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))
schemaPeople = sqlContext.createDataFrame(people)
schemaPeople.registerTempTable("people")
schemaPeople.write.parquet("people.parquet")