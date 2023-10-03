from mrjob.job import MRJob   # MRJob version

# Choose a good  class name!!
class FlightCount(MRJob):  #MRJob version

    def mapper(self, key, line):
        pass

    def reducer(self, key, values):
        # note you can only access the contents of values once, so save what you need in a variable
        pass

if __name__ == '__main__':
    FlightCount.run()   # MRJob version
