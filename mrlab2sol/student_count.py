# The following two lines are necessary so that python
# knows where to find the mockmr directory that has the
# mockmr.py file so that we can import it.
# "../mockmr/" means "go back on directory and then go to the mockmr folder"
import sys
sys.path.append("../mockmr/")

from mockmr import MockMR

BUILDING = "b"
STUDENT = "s"

class StudentCount1(MockMR):

    def mapper(self, key, value):
        if not (value.startswith("Course Name") or value.startswith("Name")):
            parts = value.split("\t")
            if len(parts) == 2:
                course = parts[1]
                yield course, (1, STUDENT)
            elif len(parts) == 3:
                course = parts[0]
                building = parts[1]
                yield course, (building, BUILDING) 

    def reducer(self, key, values_iterator):
        building = None
        sizecount = 0
        for item in values_iterator: #item is either (1, STUDENT) or (building, BUILDING))
            tag = item[1]
            if tag == BUILDING:
                building = item[0]
            else:
                increment = item[0]
                sizecount = sizecount + increment
        yield building, sizecount


if __name__ == "__main__":
    StudentCount1.run(trace=True)
