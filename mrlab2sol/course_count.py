# The following two lines are necessary so that python
# knows where to find the mockmr directory that has the
# mockmr.py file so that we can import it.
# "../mockmr/" means "go back on directory and then go to the mockmr folder"
import sys
sys.path.append("../mockmr/")

from mockmr import MockMR

class CourseCount(MockMR):

    def mapper(self, key, value):
        if not (value.startswith("Course Name") or value.startswith("Name")):
            parts = value.split("\t")
            if len(parts) == 2:
                course = parts[1]
                yield (course, 1)

    def reducer(self, key, values_iterator):
        yield key, sum(values_iterator) 


if __name__ == "__main__":
    CourseCount.run(trace=True)

