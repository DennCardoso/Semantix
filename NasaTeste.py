
# coding: utf-8
# Iniciando a biblioteca SparkContext do Pyspark
from pyspark import SparkContext


# Iniciando o SparkContext na variavel sc
sc = SparkContext()


# Obtendo as fontes de dados dos arquivos baixandos do site http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html
SourceRDD1 = sc.textFile("/Users/denncardoso/PycharmProjects/untitled/venv/Files/access_log_Aug95")
SourceRDD2 = sc.textFile("/Users/denncardoso/PycharmProjects/untitled/venv/Files/access_log_Jul95")

#União dos RDD SourceRDD1 e SourceRDD2
SourceNasa = SourceRDD1.union(SourceRDD2)


# Map and reduceByKey
ImpressNasa = SourceNasa.map(lambda x: x.split(" ")).reduceByKey(lambda x, n: x + n)
ImpressNasa.collect()


Result = SourceNasa.flatMap(lambda x: x).reduceByKey(lambda x, y: x + y)
Result.Count()

