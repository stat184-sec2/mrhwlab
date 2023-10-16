from mrjob.job import MRJob


class FlightCount(MRJob):

    def mapper(self, _, line):
        # Split the line into fields
        fields = line.split('\t')  # Adjust the delimiter if needed

        # Extract relevant information from the line
        airport_code = fields[0]  # Replace with the actual index of the airport code in your data
        passengers = int(fields[1])  # Replace with the actual index of the passengers field in your data

        # Emit key-value pairs for arrivals and departures
        yield airport_code, ('arrive', passengers)  # You may need to adjust the format here
        yield airport_code, ('leave', passengers)   # You may need to adjust the format here

    def reducer(self, key, values):
        arrive_count = 0
        leave_count = 0

        for action, passengers in values:
            if action == 'arrive':
                arrive_count += passengers
            elif action == 'leave':
                leave_count += passengers

        # Emit the total counts for arrivals and departures
        yield key, f"(arrive: {arrive_count}, leave: {leave_count})"


if __name__ == '__main__':
    FlightCount.run()
