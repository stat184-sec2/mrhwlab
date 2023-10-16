from mrjob.job import MRJob

class RetailItemStatsFast(MRJob):

    def mapper(self, _, line):
        # Parse the line and extract relevant information
        fields = line.split('\t')
        item = fields[0]  # Replace with the actual index of the item in your data
        quantity = int(fields[1])  # Replace with the actual index of the quantity field in your data
        unit_price = float(fields[2])  # Replace with the actual index of the unit price field in your data

        # Emit key-value pairs for each item: (item, (quantity, unit_price))
        yield item, (quantity, unit_price)

    def reducer_init(self):
        # Initialize variables for in-memory combining
        self.current_key = None
        self.total_quantity = 0
        self.max_unit_price = 0

    def reducer(self, key, values):
        # Accumulate quantity and find the maximum unit price in-memory
        for quantity, unit_price in values:
            self.total_quantity += quantity
            self.max_unit_price = max(self.max_unit_price, unit_price)

        # Emit the results: (item, (total_quantity, max_unit_price))
        yield key, (self.total_quantity, self.max_unit_price)

    def reducer_final(self):
        # Reset variables for the next key
        self.current_key = None
        self.total_quantity = 0
        self.max_unit_price = 0

if __name__ == '__main__':
    RetailItemStatsFast.run()

