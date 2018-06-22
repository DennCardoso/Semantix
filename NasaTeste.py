
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


# Exercicio 2
# mapeando as linhas do arquivo, usando o separador espaço
teste = SourceNasa.map(lambda x:x.split(" "))

# Criando uma tupla com a primeira coluna de Host (Host,1) e fazendo o count por reducebykey
teste2 = teste.map(lambda x:(x[8],1)).filter(lambda x:x[0]=='404').count()


# Exercicio 3
# mapeando as linhas do arquivo, usando o separador espaço
NasaLista = SourceNasa.map(lambda x:x.split(" "))

# Criando uma tupla com a URL e codigo de retorno HTTP. Além disso, realiza o filtro para apenas tuplas com erro 404
NasaErro = NasaLista.map(lambda x:(x[0],x[8])).filter(lambda x:x[0]=='404')

#Com o resultado das URL correspondentes, cria-se uma tupla com a (URL,1) e obtem-se o count.
NasaResposta = NasaErro.map(lambda x:(str(x[0]),1)).reduceByKey(lambda x,y:x+y).count()


# Exercicio 4
# mapeando as linhas do arquivo, usando o separador espaço
NasaLista = SourceNasa.map(lambda x:x.split(" "))

# Criando uma tupla com a URL e codigo de retorno HTTP. Além disso, realiza o filtro para apenas tuplas com erro 404
NasaErro = NasaLista.map(lambda x:(x[0],x[8])).filter(lambda x:x[0]=='404')

#Gerar uma nova tupla, desta vez com o resultado do agrupamento da URL com a quantidade de vezes que apresentou pagina 404
Resposta = NasaErro.map(lambda x:(x[0],1)).reduceByKey(lambda x,y:x+y)

#Resolução em texto: Necessário realizar sortByKey no RDD "resposta" decrescente para identificar os 5 casos com mais erro 404

#Exercicio 5 - Não desenvolvido

