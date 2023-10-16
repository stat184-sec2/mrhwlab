from mrjob.job import MRJob

class WordCountWithMostFrequent(MRJob):

    def mapper(self, _, line):
        words = line.split()
        for w in words:
            yield (w, 1)

    def reducer_init(self):
        self.current_word = None
        self.current_count = 0
        self.most_frequent_word = None
        self.most_frequent_count = 0

    def reducer(self, key, values):
        word_count = sum(values)

        # Standard word count emission
        yield (key, word_count)

        # Most frequent word tracking
        if word_count > self.most_frequent_count:
            self.most_frequent_word = key
            self.most_frequent_count = word_count

    def reducer_final(self):
        # Emit the most frequent word at the end of each reducer
        if self.most_frequent_word is not None:
            yield ('MostFrequent', self.most_frequent_word)

if __name__ == '__main__':
    WordCountWithMostFrequent.run()

