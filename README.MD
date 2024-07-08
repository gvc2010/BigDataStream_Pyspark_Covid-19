## Análise de Dados do Covid-19 no Brasil com PySpark Streaming

DESCRIÇÃO:
Este projeto utiliza o PySpark Streaming para realizar a análise de um conjunto de dados sobre Covid-19 no Brasil, fornecendo insights em tempo real sobre diferentes aspectos da pandemia. A análise é realizada no Google Colab. Os dados são provenientes de um arquivo CSV (covid.csv) que contém informações sobre casos de Covid-19 no Brasil. Os campos relevantes incluem:
```bash
dataNascimento: Data de nascimento do paciente

sintomas: Sintomas apresentados pelo paciente

profissionalSaude: Indica se o paciente é um profissional de saúde

cbo: Ocupação do paciente

condicoes: Condições pré-existentes do paciente

estadoTeste: Estado onde o teste foi realizado

dataTeste: Data em que o teste foi realizado

tipoTeste: Tipo de teste realizado

resultadoTeste: Resultado do teste (Positivo/Negativo)

paisOrigem: País de origem do paciente

sexo: Sexo do paciente

bairro: Bairro do paciente

estado: Estado do paciente

estadoIBGE: Código IBGE do estado do paciente

municipio: Município do paciente

municipioIBGE: Código IBGE do município do paciente

cep: CEP do paciente

origem: Origem do paciente

cnes: Código da unidade de saúde

estadoNotificacao: Estado da notificação

estadoNotificacaoIBGE: Código IBGE do estado da notificação

municipioNotificacao: Município da notificação

municipioNotificacaoIBGE: Código IBGE do município da notificação

numeroNotificacao: Número da notificação

excluido: ID de exclusão

validado: Local de validação

idade: Idade do paciente

dataEncerramento: Data de encerramento da avaliação do paciente

evolucaoCaso: Evolução do caso do paciente

classificacaoFinal: Avaliação final do caso
```

Funcionalidades do Código

Configuração do Ambiente

## Instalação das Dependências:

Instalação da biblioteca pyspark para trabalhar com Spark no Google Colab.

Configuração do PySpark:

Configuração do contexto do Spark e do StreamingContext com intervalos de processamento de 10 segundos.
Simulação de Streaming
Simulação de Envio de Dados:
Os dados do arquivo covid.csv são carregados e enviados para o Spark Streaming em lotes de 10 linhas, simulando um fluxo de dados contínuo.
Processamento e Análises em Tempo Real
Quantidade de Pacientes Positivos:

Conta o número de pacientes com teste positivo para Covid-19 no último minuto, com atualizações a cada 30 segundos.
Pacientes por Sexo e Resultado do Teste:

Conta o número de pacientes de acordo com o sexo e o resultado do teste nos últimos 50 segundos, com atualizações a cada 20 segundos.
Sintomas Comuns em Casos Positivos:

Identifica os sintomas mais comuns entre os pacientes com teste positivo para Covid-19 no último minuto, com atualizações a cada 30 segundos.
Casos Positivos no Paraná:

Conta o número de casos positivos no estado do Paraná nos últimos 40 segundos, com atualizações a cada 20 segundos.
Idade das Mulheres Positivas:

Lista a idade das mulheres que testaram positivo para Covid-19.
Município do Paraná com Mais Casos Positivos em Mulheres:

Identifica o município do Paraná com o maior número de mulheres positivas para Covid-19 no último minuto, com atualizações a cada 20 segundos.
Dia da Semana com Mais Testes Realizados:

Determina o dia da semana com a maior quantidade de testes realizados nos últimos dois minutos, com atualizações a cada 40 segundos.
Execução do Streaming
Iniciar o Streaming:
O streaming é iniciado e executado por um período de 120 segundos, permitindo que as consultas sejam processadas e atualizadas conforme os dados chegam.

## Licença

Este projeto está licenciado sob a licença MIT - veja o arquivo [LICENSE](LICENSE) para mais detalhes.

