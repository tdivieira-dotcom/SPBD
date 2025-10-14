from mrjob.job import MRJob, MRStep
from statistics import mean

class MRWebLogStats2b_V1(MRJob):

  def mapper(self, _, line):
      parts = line.split()
      if len(parts) == 6:   # make sure the line is complete
            timestamp = parts[0]
            execution_time = float(parts[5])

            time_interval_10s = timestamp[0:18] #tempo é o timestamp da posição 0 à 18
            yield time_interval_10s, execution_time   #associar à key que guarda 1 segundo o execution_time daquela request 

  def reducer(self, interval, execution_times):
      values = list(execution_times)
      yield interval, "count: {}, min: {}, max: {}, avg: {}".format(len(values), min(values), max(values), mean(values))

if __name__ == '__main__':
    MRWebLogStats2b_V1.run()
