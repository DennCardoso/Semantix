
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


# Exercicio 1
#mapeando as linhas do arquivo, usando o separador espaço
teste = SourceNasa.map(lambda x:x.split(" "))

#Criando uma tupla com a primeira coluna de Host (Host,1) e fazendo o count por reducebykey
teste2 = teste.map(lambda x:(x[0],1)).reduceByKey(lambda x,y: x+y)

# Exibe a contagem de host unicos na estrutura
teste2.filter(lambda x: x[1]==1).count()

#Exercicio 2
# mapeando as linhas do arquivo, usando o separador espaço
teste = SourceNasa.map(lambda x:x.split(" "))

# Criando uma tupla com a primeira coluna de Host (Host,1) e fazendo o count por reducebykey
teste2 = teste.map(lambda x:(x[8],1)).reduceByKey(lambda x,y: x+y)
teste2.collect()

#Exercicio 3
# mapeando as linhas do arquivo, usando o separador espaço
teste = SourceNasa.map(lambda x:x.split(" "))

# Criando uma tupla com a primeira coluna de Host (Host,1) e fazendo o count por reducebykey
teste2 = teste.map(lambda x:(x[0],x[8])).filter(lambda x:x[1]=='404').map(lambda x:(x[0],1))
teste2.count()

