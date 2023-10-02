# The following two lines are necessary so that python
# knows where to find the mockmr directory that has the
# mockmr.py file so that we can import it.
# "../mockmr/" means "go back on directory and then go to the mockmr folder"
import sys
sys.path.append("../mockmr/")

import hashlib

NUM_REDUCERS = 5 # make sure runfile uses same number, we need this so mapper_final knows how many keys to make
TOTAL_KEY_PREFIX = "__TOTAL__"

from mockmr import MockMR

class WordFreq(MockMR):

    def mapper_init(self):
        self.total = 0

    def mapper(self, key, value):
        words = value.split()
        for w in words:
            self.total += 1
            yield  w,1

    def mapper_final(self):
        for i in range(NUM_REDUCERS):
           mykey = TOTAL_KEY_PREFIX + str(i)
           yield mykey, self.total  #we want reducer i to see this, so we will modify partitioner 


    def reducer(self, key, values_iterator):
        yield key, sum(values_iterator) 


    def partition(self, key, numreducers):
        # send "__Total__i" to reducer i
        # otherwise if the key is a word, send it as usual
        thehash = int(hashlib.sha1(bytes(str(key),'utf-8')).hexdigest(), 16)
        if key.startswith(TOTAL_KEY_PREFIX):
            # now we know it is a Total_i type message:
            suffix = key[len(TOTAL_KEY_PREFIX):]
            reducer_number = int(suffix)
        else:
            reducer_number = thehash % numreducers
        return reducer_number

if __name__ == "__main__":
    WordFreq.run(trace=True)
