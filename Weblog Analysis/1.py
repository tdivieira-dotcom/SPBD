from mrjob.job import MRJob
from mrjob.step import MRStep

class MRUniqueIPs(MRJob):

    def mapper(self, _, line):
        tokens = line.split('\t')
        ip = tokens[1]  
        yield ip, 1  # Cada IP lido fica com 1

    def reducer(self, ip, unique):
        yield ip, 1  # Cada key fica com valor 1(em vez de somar)

    def reducer_sum(self, key, values):
        yield "total_unique_ips", sum(values) # Criamos esta key que tem como value a soma de todos os 1 relativo a cada ip único


    # Define a sequência de passos (Map → Reduce → Reduce)
    def steps(self):
        return [
            self.mr(mapper=self.mapper, reducer=self.reducer),
            self.mr(reducer=self.reducer_sum)
        ]


if __name__ == '__main__':
    MRUniqueIPs.run()
