# **# Nome do campo Descrição**
"""
3 dataNascimento Data de nascimento

4 sintomas Sintomas do paciente

5 profissionalSaude Relacionado a profissional de saúde

6 cbo Ocupação

7 condicoes Condições do paciente

8 estadoTeste Estado do teste

9 dataTeste Data do teste

10 tipoTeste Tipo de teste realizado

11 resultadoTeste Resultado do teste

12 paisOrigem País de origem do paciente

13 sexo Sexo do paciente

14 bairro Bairro do paciente

15 estado Estado do paciente

16 estadoIBGE Estado do paciente IBGE

17municipio Município do paciente

18municipioIBGE Município do paciente IBGE

19 cep CEP

20 origem Origem do paciente

21 cnes Código da unidade de saúde

22 estadoNotificacao Estado da notificação

23 estadoNotificacaoIBGE Estado da notificação IBGE

24municipioNotificacao Município da notificação

25municipioNotificacaoIBGE Município da notificação IBGE

26 numeroNotificacao Número da notificação

27 excluido ID de exclusão

28 validado Local de validação

29 idade Idade do paciente

30 dataEncerramento Data do encerramento da avaliação do paciente

31 evolucaoCaso Evolução do caso do paciente

32 classificacaoFinal Avaliação final do caso
"""

# Instalar as dependências no Google Colab
!pip install pyspark

# Configurar o PySpark no Google Colab
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import pandas as pd
import time

sc = SparkContext("local[2]", "NetworkWordCount")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 10)  # Intervalo de 10 segundos para o StreamingContext
ssc.checkpoint("/tmp/checkpoint")

# Função para simular o envio de dados de um arquivo CSV para o DStream
def simulate_stream(ssc, data):
    rdd_queue = []
    for i in range(0, len(data), 10):  # Dividindo os dados em pedaços de 10 linhas
        rdd_queue += [ssc.sparkContext.parallelize(data[i:i + 10])]
        time.sleep(0.1)  # Atraso de 100ms entre cada lote de dados
    return rdd_queue

# Carregar o CSV
df = pd.read_csv('covid.csv', sep=';')
data = df.to_numpy().tolist()  # Converter DataFrame para lista de listas

rdd_queue = simulate_stream(ssc, data)
input_stream = ssc.queueStream(rdd_queue)

# Função para obter o dia da semana a partir da data
import datetime

def get_day_of_week(date_str):
    date = datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    return date.strftime('%A')

# 1. Quantidade de pacientes positivos para coronavírus no último minuto e atualização a cada 30 segundos
positive_cases = input_stream.filter(lambda line: line[11] == 'Positivo')\
                             .countByWindow(60, 30)
positive_cases.pprint()

# 2. Quantidade de pacientes de acordo com o sexo e o resultado do teste nos últimos 50 segundos e atualização a cada 20 segundos
sex_test_result = input_stream.map(lambda line: ((line[13], line[11]), 1))\
                              .reduceByKeyAndWindow(lambda a, b: a + b, 50, 20)
sex_test_result.pprint()

# 3. Sintomas mais comuns para casos positivos para coronavírus no último minuto e atualização a cada 30 segundos
common_symptoms = input_stream.filter(lambda line: line[11] == 'Positivo')\
                              .flatMap(lambda line: line[4].split(','))\
                              .map(lambda symptom: (symptom, 1))\
                              .reduceByKeyAndWindow(lambda a, b: a + b, 60, 30)\
                              .transform(lambda rdd: rdd.sortBy(lambda x: -x[1]))
common_symptoms.pprint()

# 4. Quantidade de casos positivos no Paraná nos últimos 40 segundos e atualização a cada 20 segundos
positive_cases_pr = input_stream.filter(lambda line: line[15] == 'PARANÁ' and line[11] == 'Positivo')\
                                .countByWindow(40, 20)
positive_cases_pr.pprint()

# 5. Idade das mulheres positivas para Covid-19
female_positive_ages = input_stream.filter(lambda line: line[13] == 'Feminino' and line[11] == 'Positivo')\
                                   .map(lambda line: int(line[29]))  # Convertendo idade para inteiro
female_positive_ages.pprint()

# 6. Município do Paraná com a maior quantidade de mulheres positivadas para Covid-19 no último minuto e atualização a cada 20 segundos
positive_females_pr = input_stream.filter(lambda line: line[15] == 'PARANÁ' and line[13] == 'Feminino' and line[11] == 'Positivo')\
                                  .map(lambda line: (line[17], 1))\
                                  .reduceByKeyAndWindow(lambda a, b: a + b, 60, 20)\
                                  .transform(lambda rdd: rdd.sortBy(lambda x: -x[1]))
positive_females_pr.pprint()

# 7. Dia da semana com a maior quantidade de testes realizados nos últimos dois minutos e atualização a cada 40 segundos
day_of_week_tests = input_stream.map(lambda line: (get_day_of_week(line[1]), 1))\
                                .reduceByKeyAndWindow(lambda a, b: a + b, 120, 40)\
                                .transform(lambda rdd: rdd.sortBy(lambda x: -x[1]))
day_of_week_tests.pprint()

# Iniciar o streaming e aguardar a conclusão
ssc.start()
ssc.awaitTerminationOrTimeout(120)
ssc.stop(stopSparkContext=True, stopGraceFully=False)
