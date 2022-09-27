from mrjob.job import MRJob   # MRJob version

class Orders0(MRJob):  #MRJob version
    def mapper(self, key, line):
        parts = line.split("\t")
        quantity = float(parts[3])
        unitprice = float(parts[5])
        country = parts[7]
        yield (country, quantity * unitprice)

    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    Orders0.run()   # MRJob version
