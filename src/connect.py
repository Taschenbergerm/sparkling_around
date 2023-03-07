import pyspark

if __name__ == "__main__":

    conf = pyspark.SparkConf()
    conf.setMaster('spark://localhost:7077')
    sc = pyspark.SparkContext(conf=conf)
    spark = pyspark.sql.SparkSession(sc)
    df = spark.read.format("jdbc").options(url="jdbc:postgresql://demo-db:5432/public", dbtable="experiments").load()
    print(df.take(5))
