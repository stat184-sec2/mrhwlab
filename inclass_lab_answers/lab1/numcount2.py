from mrjob.job import MRJob   # MRJob version


#### Don't want words with 2 or fewer letters

class NumCount2(MRJob):  #MRJob version
    def mapper(self, key, line):
        words = line.split()
        for w in words:
            if len(w) > 2:
               yield (len(w), 1)

    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    NumCount2.run()   # MRJob version
