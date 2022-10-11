from mrjob.job import MRJob   # MRJob version

COUNTER_GROUP = "MyErrors"
COUNTER = "ConversionError"

class Orders1(MRJob):  #MRJob version
    def mapper(self, key, line):
        parts = line.split("\t")
        try:
            float(parts[3])
            float(parts[5])
            self.set_status("Alive!")
        except:
            self.increment_counter(COUNTER_GROUP, COUNTER, 1)
       

    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    Orders1.run()   # MRJob version
