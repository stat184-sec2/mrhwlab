# The following two lines are necessary so that python
# knows where to find the mockmr directory that has the
# mockmr.py file so that we can import it.
# "../mockmr/" means "go back on directory and then go to the mockmr folder"
import sys
sys.path.append("../mockmr/")

from mockmr import MockMR

class LetterCount(MockMR):

    def mapper(self, key, line):
        for mychar in line:
            if mychar.isalpha():
                yield mychar.lower(), 1  # only 26 possible keys

    def reducer(self, key, values_iterator):
        yield key, sum(values_iterator) 
        # since we have only 26 possible keys, and they are distributed among
        # different reducers, it means here the total number of output keys is
        # at most 26. So use 1 reducer?


if __name__ == "__main__":
    LetterCount.run(trace=True)
