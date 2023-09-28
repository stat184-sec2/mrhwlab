# The following two lines are necessary so that python
# knows where to find the mockmr directory that has the
# mockmr.py file so that we can import it.
# "../mockmr/" means "go back on directory and then go to the mockmr folder"
import sys
sys.path.append("../mockmr/")

from mockmr import MockMR


class StudentCountFinish(MockMR):

    def mapper(self, key, value):
        #key is building, value is a number of students
        yield key, int(value) #make sure it is an int so we can add in reducer

    def reducer(self, key, values_iterator):
        yield key, sum(values_iterator)


if __name__ == "__main__":
    StudentCountFinish.run(trace=True, input_kv=True)
