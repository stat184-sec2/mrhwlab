from mrjob.job import MRJob   # MRJob version

class Flight(MRJob):  #MRJob version
    def mapper(self, key, line):
        parts = line.split("\t")
        orgin_state = parts[?]  # fill in the number here
        destination_state = parts[?} # fill in the number here
        num_passengers = parts[?] # fill in the number here
      
        # there are num_passengers leaving the state (this is the meaning of this key,value message)
        key = origin_state
        value = (num_passenger, "Outgoing")
        yield (key, value)

        # there are num_passengers arriving in the state (this is the meaning of this key,value message)
        key = destination_state
        value = (num_passengers, "Incoming")
        yield (key, value)

    def reducer(self, key, values):
        #key would look like Fl
        # values would like [(3, "Incoming"), (4, "Outgoing"), (6, "Incoming")]
        # you should get x=total number of incoming, y=total number of outgoing
        yield (key, (x,y))

if __name__ == '__main__':
    Flight.run()   # MRJob version
