import pandas as pd
import pyspark


DATA = "/home/marvin/Projects/spark101/data/student_data.csv"


def data():
    return pd.read_csv(DATA, nrows=10)


def main():
    conf = pyspark.SparkConf()
    conf.setMaster('spark://localhost:7077')
    sc = pyspark.SparkContext(conf=conf)
    spark = pyspark.sql.SparkSession(sc)

    #Createa spark DataFrame
    df = spark.createDataFrame(data())

    # Show describtion
    df.describe().show()

    # Show summary
    df.summary().show()


    # df = spark.createDataFrame(
    #         [("Alice", 13, 40.3, 150.5), ("Alice", 12, 37.8, 142.3), ("Tom", 11, 44.1, 142.2)],
    #         ["name", "age", "weight", "height"] )
    # df.describe().show()

if __name__ == '__main__':
    main()