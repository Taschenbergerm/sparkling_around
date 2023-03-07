from loguru import logger
import pyspark
import pprint
from pyspark.sql import functions
from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType


def main():
    conf = pyspark.SparkConf()
    conf.setMaster('spark://localhost:7077')
    sc = pyspark.SparkContext(conf=conf)
    spark = pyspark.sql.SparkSession(sc)

    df = spark.createDataFrame(data=data(), schema=create_schema())
    logger.info(pprint.pformat(df. schema.jsonValue()))
    aggregate(df, "avg")
    aggregate(df, "var_samp")
    aggregate(df, "var_pop")
    df.agg(functions.avg("ZipCode"), functions.var_pop("ZipCode"), functions.var_samp("ZipCode")).show()


def aggregate(df, kpi):
    stats = df.agg({"ZipCode": kpi,})
    stats.show()


def create_schema():
    zip = StructField("Zipcode", LongType(), True)
    zip_type = StructField("ZipcodeType", StringType(), True)
    city = StructField("City", StringType(), True)
    state = StructField("State", StringType(), True)
    return StructType([zip, zip_type, city, state])


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