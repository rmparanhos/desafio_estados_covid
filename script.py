from __future__ import absolute_import

import argparse
import logging
import json

from os import listdir
from os.path import isfile, join

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class CasosSplitDoFn(beam.DoFn):
  def process(self, element):
    # Extrai as infos necessarias do csv de casos
    regiao = element.split(';')[0]  
    uf = element.split(';')[1]
    cod = element.split(';')[3]
    casos_novos = element.split(";")[11]
    obitos_novos = element.split(";")[13]
    dicionario = [{
            "regiao_uf_cod": regiao + ";" + uf + ";"+ cod,
            "casos_novos": int(casos_novos),
            "obitos_novos": int(obitos_novos),            
          }]      
    return dicionario

class CasosToTupleDoFn(beam.DoFn):
  def process(self, element):  
      # Retorna o dicionario no formato de tupla
      result = [(element['regiao_uf_cod'], (element['casos_novos'], element['obitos_novos']))]
      return result

def run(argv=None, save_main_session=True):
  """Main entry point"""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      required=True,
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  #limpa csv do covid pois como estamos somando por estado, nao precisamos das infos por municipio nem por pais
  with open(known_args.input + "/HIST_PAINEL_COVIDBR_28set2020.csv", 'r') as infile, open(known_args.input + "/HIST_PAINEL_COVIDBR_28set2020_clean.csv", 'w') as outfile:
        count = 1
        for line in infile:
            if count >= 219 and count <= 6077:
                outfile.write(line)
            count += 1

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:

    # Carrega os casos numa PCollection.
    casos = p  | 'Read Casos' >> ReadFromText(known_args.input + "HIST_PAINEL_COVIDBR_28set2020_clean.csv")
    
    # Agregador customizado para somar tanto os casos quanto os obitos
    def multi_sum(element):
      (key, values) = element
      #transposta para aplicar o sum
      values_t = list(zip(*values))
      return (key, (sum(values_t[0]), sum(values_t[1])))

    # Obtem as contagens
    counts = ( 
      casos
      | 'SplitToDict' >> beam.ParDo(CasosSplitDoFn())
      | 'DictToTuple' >> beam.ParDo(CasosToTupleDoFn())
      | 'Group' >> beam.GroupByKey()
      | 'Count' >> beam.Map(multi_sum))
     
    # Formata os resultados obtidos conforme pedido para o csv
    def format_result_csv(regiao_uf_cod, count):
      nome = ""
      governador = ""
      regiao = regiao_uf_cod.split(';')[0]  
      uf = regiao_uf_cod.split(';')[1]
      cod = regiao_uf_cod.split(';')[2]
      with open(known_args.input + "EstadosIBGE.csv", 'r') as infile:
        for line in infile:
          if cod == line.split(";")[1]:
            nome = line.split(";")[0]
            governador = line.split(";")[3]
      return '%s,%s,%s,%s,%d,%d' % (regiao, nome, uf, governador, count[0], count[1])

    # Formata os resultados obtidos conforme pedido para o JSON
    def format_result_json(regiao_uf_cod, count):
      nome = ""
      governador = ""
      regiao = regiao_uf_cod.split(';')[0]  
      uf = regiao_uf_cod.split(';')[1]
      cod = regiao_uf_cod.split(';')[2]
      with open(known_args.input + "/EstadosIBGE.csv", 'r') as infile:
        for line in infile:
          if cod == line.split(";")[1]:
            nome = line.split(";")[0]
            governador = line.split(";")[3]
      dict_j = {"Regiao":regiao,
              "Estado":nome,
              "UF":uf,
              "Governador":governador,
              "TotalCasos":count[0],
              "TotalObitos":count[1]}
      return json.dumps(dict_j, ensure_ascii=False)

    output_csv = counts | 'FormatCSV' >> beam.MapTuple(format_result_csv)
    output_json = counts | 'FormatJSON' >> beam.MapTuple(format_result_json)
    
    output_csv | 'WriteCSV' >> WriteToText(known_args.output,file_name_suffix='.csv')
    output_json | 'WriteJSON' >> WriteToText(known_args.output,file_name_suffix='.json')

  #gera lista de arquivos no diretorio para consolidar
  onlyfiles = [f for f in listdir(known_args.output) if isfile(join(known_args.output, f))]
  with open(known_args.output + "CONSOLIDADO.csv", 'w') as csv_file, open(known_args.output + "CONSOLIDADO.json", 'w') as json_file:
    #headers
    csv_file.write('Regiao,Estado,UF,Governador,TotalCasos,TotalObitos\n')
    json_file.write('[')
    output_files = [f for f in listdir(known_args.output) if isfile(join(known_args.output, f))]
    csv_files = [x for x in output_files if ".csv" in x]
    json_files = [x for x in output_files if ".json" in x]
    #para encontrar o ultimo json e fechar a lista
    count_files = 1
    for file_name in csv_files:
      with open(known_args.output + file_name, 'r') as part:
        for line in part:
          csv_file.write(line)          
    for file_name in json_files:
      with open(known_args.output + file_name, 'r') as part:
        count_lines = 1
        lines = part.readlines()
        for line in lines:
          if count_files == len(json_files) and count_lines == len(lines):
            json_file.write(line + ']')
            count_lines += 1
          else:
            json_file.write(line + ',')  
            count_lines += 1
      count_files += 1        


if __name__ == '__main__':
  #logging.getLogger().setLevel(logging.INFO)
  run()
