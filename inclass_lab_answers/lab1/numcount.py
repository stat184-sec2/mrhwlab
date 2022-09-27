from mrjob.job import MRJob   # MRJob version



# small data version
# counter = {}
# with open("wap.txt") as infile:
#     for line in wap:
#         words = line.split()
#         for w in words:
#              wlen = len(w)
#              counter[wlen] = counter.get(wlen,0) + 1


# Change the class name!!
class NumCount(MRJob):  #MRJob version
    def mapper(self, key, line):
        words = line.split()
        for w in words:
            yield (len(w), 1)

    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    NumCount.run()   # MRJob version
