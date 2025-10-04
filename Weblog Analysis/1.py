from mrjob.job import MRJob

class MRUniqueIPs(MRJob):

    def mapper(self, _, line):
        # Ignora linhas vazias ou cabeçalho
        tokens = line.strip().split('\t')

        ip = parts[1]  
        yield ip, 1  # Cada IP lido fica com 1

    def reducer(self, ip, counts):
        yield "total_unique_ips", 1  # Cada IP diferente fica com [total_unique_ips,1], [total_unique_ips,1], ...

    def reducer_sum(self, key, values):
        yield key, sum(values)    # Somamos todos os "1" para obter o total final de IPs únicos


    # Define a sequência de passos (Map → Reduce → Reduce)
    def steps(self):
        return [
            self.mr(mapper=self.mapper, reducer=self.reducer),
            self.mr(reducer=self.reducer_sum)
        ]


if __name__ == '__main__':
    MRUniqueIPs.run()
