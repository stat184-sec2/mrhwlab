from mrjob.job import MRJob   # MRJob version

class Orders3(MRJob):  #MRJob version
    def mapper(self, key, line):
        parts = line.split("\t")
        if "Invoice" not in parts[0]:
            quantity = float(parts[3])
            unitprice = float(parts[5])
            country = parts[7]
            yield (country, quantity * unitprice)

    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    Orders3.run()   # MRJob version
