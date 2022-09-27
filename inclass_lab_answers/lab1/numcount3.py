from mrjob.job import MRJob   # MRJob version


#### Don't want words with 2 or fewer letters
#### Don't want to see numbers that occur 1 or fewer times

class NumCount3(MRJob):  #MRJob version
    def mapper(self, key, line):
        words = line.split()
        for w in words:
            if len(w) > 2:
                yield (len(w), 1)

    def reducer(self, key, values):
        # values is an iterator, you can only read it once,
        # but the commented out code below reads it twice and you
        # will get the wrong answer
        #if sum(values) > 1:
        #    yield (key, sum(values))
        num = sum(values) # now we don't need to access values again
        if num > 1:
             yield (key, num)

     # we don't expect that many wordlengths, so 1 reducer can probably handle this,
     # so we need to modify the runfile to change the number of reducers from 4 to 1
     # (current runfiles did not do this)
if __name__ == '__main__':
    NumCount3.run()   # MRJob version
