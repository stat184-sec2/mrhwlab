# The following two lines are necessary so that python
# knows where to find the mockmr directory that has the
# mockmr.py file so that we can import it.
# "../mockmr/" means "go back on directory and then go to the mockmr folder"
import sys
sys.path.append("../mockmr/")

from mockmr import MockMR

class Better_LC(MockMR):

    def mapper_init(self):
        """ usually good for initializing memory the mapper will have for its shard """
        # mapper is going to remember the letters that have already been seen by it
        self.cache = {} # dictionary key will be the letter, dictionary value is the count

    def mapper(self, key, line):
        for mychar in line:
            if mychar.isalpha():
                # instead of sending a message for every char, we will update the cache instead
                self.cache[mychar] = self.cache.get(mychar, 0) + 1
                # note we are not sending any messages at all! (this is not common)
                # size of cache will always be small (at most 26 entries)

    def mapper_final(self):
        """ our last chance to send key/value pairs we haven't sent before """
        for letter in self.cache:
            yield letter, self.cache[letter]
        # only one message per letter, usually in english we have 26 of them
        # so each mapper sends at most 26 messages. 
        # so total size of messages sent to reducers is now 26 * (# of mappers)

    def reducer(self, key, values_iterator):
        yield key, sum(values_iterator) 
        # since we have only 26 possible keys, and they are distributed among
        # different reducers, it means here the total number of output keys is
        # at most 26. So use 1 reducer? Yes, because total amount/size of messages
        # sent by mappers is small, and total reducer output is small


if __name__ == "__main__":
    Better_LC.run(trace=True)
