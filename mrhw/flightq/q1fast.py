from mrjob.job import MRJob

class FlightCountFast(MRJob):

    def mapper(self, _, line):
        # Assuming the data is tab-separated, adjust the delimiter if needed
        fields = line.split('\t')

        airport_code = fields[0]  # Replace with the actual index of the airport code in your data
        passengers = int(fields[1])  # Replace with the actual index of the passengers field in your data

        # Emit key-value pairs for arrivals and departures
        yield airport_code, ('arrive', passengers)
        yield airport_code, ('leave', passengers)

    def reducer_init(self):
        # Initialize variables for in-memory combining
        self.current_key = None
        self.arrive_count = 0
        self.leave_count = 0

    def reducer(self, key, values):
        # Your reducer logic with in-memory combining goes here
        for action, passengers in values:
            if action == 'arrive':
                self.arrive_count += passengers
            elif action == 'leave':
                self.leave_count += passengers

        # Emit the final results for the current key
        if self.current_key is not None:
            yield self.current_key, f"(arrive: {self.arrive_count}, leave: {self.leave_count})"

        # Reset variables for the next key
        self.current_key = key
        self.arrive_count = 0
        self.leave_count = 0

    def reducer_final(self):
        # Emit the final results for the last key
        if self.current_key is not None:
            yield self.current_key, f"(arrive: {self.arrive_count}, leave: {self.leave_count})"

if __name__ == '__main__':
    FlightCountFast.run()

