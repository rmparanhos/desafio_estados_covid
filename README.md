# Desafio Estados X COVID-19

O desafio consiste em extrair dados relevantes de dois csvs, contendo informações relacionadas aos estados brasileiros e ao COVID-19, utilizando o [Apache Beam](https://beam.apache.org).

## Arquivos de Entrada
- **EstadosIBGE.csv** possui os dados dos estados brasileiros.
- **HIST_PAINEL_COVIDBR_28set20.csv** possui os dados do COVID-19 no Brasil.

## Arquivos de Saída
- ***-XXXXX-of-XXXX.csv**, arquivos produzidos no processamento do Beam, em um formato preliminar de csv.
- ***-XXXXX-of-XXXX.json**, arquivos produzidos no processamento do Beam, em um formato preliminar de json.
- **CONSOLIDADO.csv**, junção de todos os arquivos produzidos do tipo csv, com headers.
- **CONSOLIDADO.json**, junção de todos os arquivos produzidos do tipo json, na forma de uma lista de jsons.

## Instalação e Execução 

O pipeline foi desenvolvido em Python 3 e utilizando o Apache Beam. A lista de pacotes instalados no Virtual Environment está em **requirements.txt**. O único pacote instalado foi o Beam, porem ele possui muitas dependências, por isso o **requirements.txt**. Apesar da possibilidade de utilizar outros pacotes, como o Pandas, quis desenvolver o pipeline apenas com Beam e bibliotecas internas do Python 3.

Para executar, a partir do diretório que possui o script.py, utilize o comando:
> python -m script --input DIRETORIO-ENTRADA --output DIRETORIO-SAIDA

**IMPORTANTE:** O DIRETORIO-ENTRADA deve possuir os arquivos de entrada nomeados exatamente da mesma forma dos arquivos mencionados em **Arquivos de Entrada**. E o DIRETORIO-SAIDA não deve possuir outros arquivos alem dos produzidos pelo próprio pipeline.

## Processo
O objetivo do pipeline é produzir dois arquivos (**CONSOLIDADO.csv** e **CONSOLIDADO.json**) contendo, para cada estado do Brasil, sua região, seu nome, seu governador, o total de casos e o total de óbitos. Assim:
1. É realizado uma limpeza no arquivo **HIST_PAINEL_COVIDBR_28set20.csv**, retirando as linhas que identifiquei como desnecessárias para o desafio, isto é, linhas com informações sobre o Brasil e linhas com informações sobre cada município. O arquivo resultante é chamado **HIST_PAINEL_COVIDBR_28set20_clean.csv**.
2. O pipeline Beam é iniciado, e uma **PCollection** é criada a partir da leitura de **HIST_PAINEL_COVIDBR_28set20_clean.csv**.
3. A **PCollection** é transformada uma nova **PCollection** ,devido a imutabilidade, contendo os dados da antiga em um formato de dicionário, mas desta vez, excluindo os dados desnecessários, utilizando a classe **CasosSplitDoFN()**.
4. A **PCollection** é transformada novamente, desta vez passando do formato de dicionário para tuplas, utilizando a classe **CasosToTupleDoFN()**.
5. É realizado o agrupamento por chave, e um somatório dos casos e óbitos, com os dados extraídos das colunas **casosNovos** e **obitosNovos**, utilizando uma função costumizada de agregação **multi_sum(element)**.
6. Os dados da coleção são escritos em seus arquivos preliminares, utilizando uma formatação de csv **format_result_csv()** ou um formatação de json **format_result_json()**.
7. Finalizado o pipeline, é feito uma busca nos arquivos gerados pelo Beam, no DIRETORIO-SAIDA. Os arquivos gerados no formato .csv serão consolidados no arquivo **CONSOLIDADO.csv**, separados por virgulas, e com readers. Os arquivos gerados no formato .json serão consolidados no arquivo **CONSOLIDADO.json**, no formato de lista de jsons, que é um formato de json valido.

## Observações 
- O arquivo script.py, está comentado a fim de auxiliar no entendimento.
- O formato do **CONSOLIDADO.json** foi validado em [RFC 8259](https://jsonformatter.curiousconcept.com)
- O somatório obtido com os dados das colunas **casosNovos** e **obitosNovos**, pode ser obitido também verificando diretamente os campos **casosAcumulado** e **obitosAcumulado**. Entretanto, devido a premissa da utilização do Apache Beam, preferi realizar o somatório ao invés da opção mais direta.
- Resultados de exemplo estão em **desafio_apache_beam_-_arquivos/output**
