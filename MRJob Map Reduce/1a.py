#@title Exercise 1a)
%%file word_freq_1a.py

import string
from unidecode import unidecode # used to remove accents and such...
from mrjob.job import MRJob, MRStep

class MRWordCountFrequency1a(MRJob):

  def mapper(self, _, line):
    line=unidecode(line).translate(line.maketrans('','',string.punctuation+'«»')).lower()
    words= line.split()
    for word in words:
        yield word, 1

  def combiner(self, word, values):
      yield word, sum( values )

  def reducer(self, word, values):
    yield word, sum(  values  )

if __name__ == '__main__':
    MRWordCountFrequency1a.run()
