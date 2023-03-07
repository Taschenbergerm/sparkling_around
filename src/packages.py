
import pyspark
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql import Row


def main():
    conf = pyspark.SparkConf().setAll([
    ("spark.pyspark.virtualenv.enabled", "true"),
    ("spark.pyspark.virtualenv.bin.path", "/usr/bin/virtualenv"),
    ("spark.pyspark.python", "python3")])
    conf.setMaster('spark://localhost:7077')
    sc = pyspark.SparkContext(conf=conf)
    spark = pyspark.sql.SparkSession(sc)
    spark.sparkContext.install_pypi_package("requests")
    df = spark.createDataFrame(data=data())
    print(df.take(5))


def data():
    return [{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"},
{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PASEO COSTA DEL SUR","State":"PR"},
{"Zipcode":709,"ZipCodeType":"STANDARD","City":"BDA SAN LUIS","State":"PR"},
{"Zipcode":76166,"ZipCodeType":"UNIQUE","City":"CINGULAR WIRELESS","State":"TX"},
{"Zipcode":76177,"ZipCodeType":"STANDARD","City":"FORT WORTH","State":"TX"},
{"Zipcode":76177,"ZipCodeType":"STANDARD","City":"FT WORTH","State":"TX"},
{"Zipcode":704,"ZipCodeType":"STANDARD","City":"URB EUGENE RICE","State":"PR"},
{"Zipcode":85209,"ZipCodeType":"STANDARD","City":"MESA","State":"AZ"},
{"Zipcode":85210,"ZipCodeType":"STANDARD","City":"MESA","State":"AZ"},
{"Zipcode":32046,"ZipCodeType":"STANDARD","City":"HILLIARD","State":"FL"}]


if __name__ == '__main__':
    main()