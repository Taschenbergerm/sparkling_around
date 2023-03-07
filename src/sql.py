import json
from loguru import logger
import pyspark
import  pprint
from pyspark.sql.functions import udf, col, explode
from pyspark.sql import functions
from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType
from pyspark.sql import Row


def main():
    conf = pyspark.SparkConf()
    conf.setMaster('spark://localhost:7077')
    sc = pyspark.SparkContext(conf=conf)
    spark = pyspark.sql.SparkSession(sc)

    df = spark.createDataFrame(data=data())
    meanVal = df.agg({"iv1": "avg", "iv2": "avg", "iv3": "avg"})
    meanVal.show()
    logger.info(df.corr('iv1','iv3'))

    df.describe().show()
    df.summary().show()
    label_it_udf = udf(label_it, StringType())
    df.withColumn('iv4', label_it_udf('iv3')).show(5)
    logger.info("Print Select ")
    df.withColumn("id", functions.monotonically_increasing_id()).createOrReplaceTempView("IvValues")
    spark.udf.register("labelValue", label_it_udf)
    spark.sql("Select * from IvValues").show(5)
    logger.info("Print UDF")
    spark.sql("Select iv1, iv2, iv3, labelValue(iv3) from IvValues").show(5)
    spark.sql("Select max(iv1) from IvValues").createOrReplaceTempView("max_iv1")
    spark.sql("Select max(iv2) from IvValues").createOrReplaceTempView("max_iv2")
    logger.success("Done")


def aggregate(df, kpi):
    stats = df.agg({"ZipCode": kpi,})
    stats.show()


def label_it(x):
    if x > 10.0:
        return "High"
    else:
        return "Low"


def data():
    return [
        {"iv1":5.5,"iv2":8.5,"iv3":9.5},
        {"iv1":6.13,"iv2":9.13,"iv3":10.13},
        {"iv1":5.92,"iv2":8.92,"iv3":9.92},
        {"iv1":6.89,"iv2":9.89,"iv3":10.89},
        {"iv1":6.12,"iv2":9.12,"iv3":10.12}
    ]


if __name__ == '__main__':
    main()