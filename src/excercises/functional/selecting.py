import pandas as pd
import pyspark


DATA = "/home/marvin/Projects/spark101/data/student_data.csv"

def main():
    conf = pyspark.SparkConf()
    conf.setMaster('spark://localhost:7077')
    sc = pyspark.SparkContext(conf=conf)
    spark = pyspark.sql.SparkSession(sc)

    #Createa spark DataFrame
    df = spark.createDataFrame(data())

    # Selecting data , fields and limiting
    df.select("*").show(10)
    df.select("ID", "FirstName").show(10)
    df.select("ID", "FirstName").limit(10).show()

    # Filtering with where
    df.select("ID", "Score").where("Subject == 'Java'").show(10)

    # Filtering and ordering
    df.select("ID", "Score").where("Subject == 'Java'").orderBy("Score").show(10)


def data():
    return pd.read_csv(DATA, nrows=100)


if __name__ == '__main__':
    main()