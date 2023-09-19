# The following two lines are necessary so that python
# knows where to find the mockmr directory that has the
# mockmr.py file so that we can import it.
# "../mockmr/" means "go back on directory and then go to the mockmr folder"
import sys
sys.path.append("../mockmr/")

from mockmr import MockMR

class MyMR2(MockMR):

    def mapper(self, key, value):
        words = value.split()
        for w in words:
            yield w,1

    def reducer(self, key, values_iterator):
        yield key, sum(values_iterator) 


if __name__ == "__main__":
    MyMR2.run(trace=True)
