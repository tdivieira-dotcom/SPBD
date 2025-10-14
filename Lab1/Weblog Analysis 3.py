from mrjob.job import MRJob, MRStep

class MRWebLogStats2c_V1(MRJob):

  def mapper(self, _, line):
      tokens = line.split('\t')
      if len(tokens) == 6:   # make sure the line is complete
            timestamp = parts[0] #token da data
            ip = tokens[1]       #token do ip
            URL = tokens[4]      #token do URL
            time_interval_10s = timestamp[0:18] #dentro da data só queremos até aos segundos
            #inverted index- cada time e URL ficam com os ips em vez de cada IP ficar com URL
            yield "{}-{}".format(time_interval_10s, URL), ip   #cada URL e segundo fica com o ip de que foi acedido

  def reducer(self, interval_URL, ips):  #interval_URL é um tuple {time_interval_10s, URL}
            unique_ip= set(ips)
            yield interval_URL, unique_ip
  
if __name__ == '__main__':
    MRWebLogStats2c_V1.run()
