#@title Exercise 1c)
%%file word_freq_1c.py

import string
from unidecode import unidecode # used to remove accents and such...

from mrjob.job import MRJob, MRStep

MAX_FREQ = 1000000
class MRWordCountFrequency1c(MRJob):

  def mapper_frq(self, _, line):
    line = unidecode(line).translate(str.maketrans('', '', string.punctuation+'«»')).lower()
    words = line.split()
    for word in words:
      yield word, 1

  def combiner_frq(self, word, occurrences ):
      yield word, sum( occurrences )

  def reducer_frq(self, word, occurrences):
      yield word, sum( occurrences )

  def mapper_sort(self, word, occurrences):
      yield "{:06}".format(MAX_FREQ-int(occurrences)), word   #palavras com mais ocorrências ficam com chaves menores (999995<999998)

  def reducer_sort(self, occurrences, words):
    for word in words:
      yield word, MAX_FREQ - int(occurrences)   #com as palavras já ordenadas por ocorrência, recuperamos o seu valor real. ex: MAX_FREQ-999995=5 

  def steps(self):
    return [ MRStep(mapper=self.mapper_frq, combiner=self.combiner_frq, reducer=self.reducer_frq), #1º step o de contagem
      MRStep(mapper=self.mapper_sort, reducer=self.reducer_sort)  #2º step o de ordenação
    ]

if __name__ == '__main__':
    MRWordCountFrequency1c.run()
