%spark.pyspark
sdfact_search_base_table  = 'search.sdfact_search_base'
df = spark.table(sdfact_search_base_table).limit(10)
df.printSchema()

