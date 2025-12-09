# PySpark Mini Cheat Sheet

df = spark.read.csv(path, header=True, inferSchema=True)

df = df.withColumn("year", year("incident_date"))

df = df.filter(col("state") == "TX")

df2 = df.join(df_lookup, "county_fips", "left")

df.write.partitionBy("year").mode("overwrite").parquet(output_path)
