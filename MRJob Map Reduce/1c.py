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
      yield "{:06}".format(MAX_FREQ-int(occurrences)), word

  def reducer_sort(self, occurrences, words):
    for word in words:
      yield word, MAX_FREQ - int(occurrences)

  def steps(self):
    return [ MRStep(mapper=self.mapper_frq, combiner=self.combiner_frq, reducer=self.reducer_frq),
      MRStep(mapper=self.mapper_sort, reducer=self.reducer_sort)
    ]

if __name__ == '__main__':
    MRWordCountFrequency1c.run()
