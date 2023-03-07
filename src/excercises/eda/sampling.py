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
    print("Count: ", df.count())

    # Simple sample
    df.sample(withReplacement=False, fraction=0.2, seed=100).show()

    #Sample with replacement
    df.sample(withReplacement=False, fraction=0.2, seed=100).show()

    # Sampling with a desired distribution
    sample = df.sampleBy(col="Subject", fractions={"Java": 0.3, "Python": 0.3, "Go": 1.0}, seed=100)
    sample.show()
    print("Count2:", sample.count())
    sample.freqItems(cols=["Subject"]).show()


def data():
    return pd.read_csv(DATA, nrows=10)




if __name__ == '__main__':
    main()