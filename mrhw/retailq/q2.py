from mrjob.job import MRJob

class RetailItemStats(MRJob):

    def mapper(self, _, line):
        # Parse the line and extract relevant information
        fields = line.split('\t')
        item = fields[0]  # Replace with the actual index of the item in your data
        quantity = int(fields[1])  # Replace with the actual index of the quantity field in your data
        unit_price = float(fields[2])  # Replace with the actual index of the unit price field in your data

        # Emit key-value pairs for each item: (item, (quantity, unit_price))
        yield item, (quantity, unit_price)

    def reducer(self, key, values):
        # Accumulate quantity and find the maximum unit price
        total_quantity = 0
        max_unit_price = 0

        for quantity, unit_price in values:
            total_quantity += quantity
            max_unit_price = max(max_unit_price, unit_price)

        # Emit the results: (item, (total_quantity, max_unit_price))
        yield key, (total_quantity, max_unit_price)

if __name__ == '__main__':
    RetailItemStats.run()
