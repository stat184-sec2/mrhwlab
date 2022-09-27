from mrjob.job import MRJob   # MRJob version

class Orders2(MRJob):  #MRJob version
    def mapper(self, key, line):
        parts = line.split("\t")
        try:
            float(parts[3])
            float(parts[5])
            self.set_status("Alive!")
        except:
            yield (parts[0], "|".join(parts))

    def reducer(self, key, values):
        yield (key, "!".join(values))

if __name__ == '__main__':
    Orders2.run()   # MRJob version
