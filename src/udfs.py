from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf


def celsiustoFahrenheit(temp):
    return ((temp*9.0/5.0)+32)


def labelTemprature(temp):
    if temp > 12.9 :
        return "High"
    else :
        return "Low"

celsiustoFahrenheit(12.2)
celsiustoFahrenheitUdf = udf("celsiustoFahrenheit", DoubleType())
