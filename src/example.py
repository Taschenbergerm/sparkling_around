import pyspark

if __name__ == "__main__":
    sc = pyspark.SparkContext('localhost:7077')

    txt = sc.textFile('file:///tmp/hello.txt')
    print(txt.count())

    python_lines = txt.filter(lambda line: 'python' in line.lower())
    print(python_lines.count())
