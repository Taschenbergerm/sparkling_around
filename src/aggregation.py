import json
from loguru import logger
import pyspark
import  pprint
from pyspark.sql.functions import udf, col, explode
from pyspark.sql import functions
from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType, IntegerType
from pyspark.sql import Row


HEADER = {
        'X-RapidAPI-Key': "",
        'X-RapidAPI-Host': ""
    }


def main():
    conf = pyspark.SparkConf()
    conf.setMaster('spark://localhost:7077')
    sc = pyspark.SparkContext(conf=conf)
    spark = pyspark.sql.SparkSession(sc)
    data = load_city_data()

    df = spark.createDataFrame(data=data)
    df.summary().show()
    df.createTempView("city_data")
    spark.sql("select * from city_data").show(5)

    logger.info("UDF with requests ")
    city_udf = udf(load_city_details, IntegerType())
    spark.udf.register("city_details", city_udf)
    spark.sql("select * , city_details(wikiDataId) as population2 from city_data").show(5)


def load_city_details(id: str):
    import http.client

    conn = http.client.HTTPSConnection("wft-geo-db.p.rapidapi.com")

    headers = {
        'X-RapidAPI-Key': "37635e69bfmsha52e3f919b54af7p1e296bjsn3fba5c8c3dc2",
        'X-RapidAPI-Host': "wft-geo-db.p.rapidapi.com"
    }

    conn.request("GET", f"/v1/geo/cities/{id}", headers=headers)

    res = conn.getresponse()
    data = res.read()
    return json.loads(data.decode("utf-8")).get("data").get("population")


def load_city_data():
    import http.client

    conn = http.client.HTTPSConnection("wft-geo-db.p.rapidapi.com")
    conn.request("GET", "/v1/geo/cities?countryIds=DE", headers=HEADER)
    res = conn.getresponse()
    data = res.read()
    return json.loads(data.decode("utf-8")).get("data")


def load_country_data():
    import http.client

    conn = http.client.HTTPSConnection("wft-geo-db.p.rapidapi.com")
    conn.request("GET", "/v1/geo/countries/DE", headers=HEADER)
    res = conn.getresponse()
    data = res.read()
    return json.loads(data.decode("utf-8")).get("data")

if __name__ == '__main__':
    main()