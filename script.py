from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class EstadosExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""
  def process(self, element):
    """Returns an iterator over the words of this element.

    The element is a line of text.  If the line is blank, note that, too.

    Args:
      element: the element being processed

    Returns:
      The processed element.
    """
    info = []
    count = 0
    for item in element.split(";"):
      if count == 0: #Nome do estado
        info.append(item)
      elif count == 3: #Nome do governador
        info.append(item)
      count += 1    
    return info

class CasosSplitDoFn(beam.DoFn):
  def process(self, element):
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
    # Returns a list of tuples containing Date and Open value
        result = [(element['regiao_uf_cod'], (element['casos_novos'], element['obitos_novos']))]
        print(result)
        return result

def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflow-samples/shakespeare/kinglear.txt',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  #limpa csv do covid pois como estamos somando por estado, nao precisamos das infos por municipio
  with open(known_args.input + "/HIST_PAINEL_COVIDBR_28set2020.csv", 'r') as infile, open(known_args.input + "/HIST_PAINEL_COVIDBR_28set2020_clean.csv", 'w') as outfile:
        count = 1
        for line in infile:
            if count == 1 or count <= 6077:
                outfile.write(line)
            count += 1

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    estados = p | 'Read_estados' >> ReadFromText(known_args.input + "/EstadosIBGE.csv")
    casos = p  | 'Read_casos' >> ReadFromText(known_args.input + "/HIST_PAINEL_COVIDBR_28set2020_clean.csv",skip_header_lines=1)
    
    def multi_sum(element):
      (key, values) = element
      values_t = list(zip(*values))
      return (key, (sum(values_t[0]), sum(values_t[1])))

    counts = ( 
      casos
      | 'SplitToDict' >> beam.ParDo(CasosSplitDoFn())
      | 'DictToTuple' >> beam.ParDo(CasosToTupleDoFn())
      | 'group' >> beam.GroupByKey()
      | 'count' >> beam.Map(multi_sum))
      #| 'GroupAndSum' >> beam.CombinePerKey(sum))           
    
    #counts = (
    #    lines
    #    | 'Split' >>
    #    (beam.ParDo(EstadosExtractingDoFn()).with_output_types(unicode))
    #    | 'PairWIthOne' >> beam.Map(lambda x: (x, 1))
    #    | 'GroupAndSum' >> beam.CombinePerKey(sum))
    
    # Format the counts into a PCollection of strings.
    def format_result(regiao_uf_cod, count):
      nome = ""
      governador = ""
      regiao = regiao_uf_cod.split(';')[0]  
      uf = regiao_uf_cod.split(';')[1]
      cod = regiao_uf_cod.split(';')[2]
      with open(known_args.input + "/EstadosIBGE.csv", 'r') as infile:
        for line in infile:
          print(cod)
          print(line.split(";")[1])
          if cod == line.split(";")[1]:
            nome = line.split(";")[0]
            governador = line.split(";")[3]
      return '%s,%s,%s,%s,%d,%d' % (regiao, nome, uf, governador, count[0], count[1])

    output = counts | 'Format' >> beam.MapTuple(format_result)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | 'WriteCSV' >> WriteToText(known_args.output)

    output | 'WriteJSON' >> WriteToText(known_args.output)


if __name__ == '__main__':
  #logging.getLogger().setLevel(logging.INFO)
  run()
