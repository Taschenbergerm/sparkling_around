import pandas as pd
import pyspark
from pyspark.sql import functions

DATA = "/home/marvin/Projects/spark101/data/student_data.csv"


def data():
    return pd.read_csv(DATA, nrows=1000)


def main():
    conf = pyspark.SparkConf()
    conf.setMaster('spark://localhost:7077')
    sc = pyspark.SparkContext(conf=conf)
    spark = pyspark.sql.SparkSession(sc)

    #Createa spark DataFrame
    df = spark.createDataFrame(data())

    df.select("Score").where("Subject = 'Python'").agg({"Score": "avg"}).show()

    df.select("Score").where("Subject = 'Python'").agg({"Score": "avg", "Score": "max"}).show()

    df.agg(functions.avg("Score"), functions.var_pop("Score"), functions.var_samp("Score")).show()

if __name__ == '__main__':
    main()