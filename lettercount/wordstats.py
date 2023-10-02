import sys
sys.path.append("../mockmr/")

import hashlib
from mockmr import MockMR

CACHE_SIZE = 5 # 5 is way too small in practice, but for our small input test data
              # we need a small number in order to ensure tha cache fills up (so the 
              # cache size limiting part of the code can be tested)
TOTAL_KEY = "Total_" # this is the key we wil use to keep track of the total.

class WordStats(MockMR):

   def mapper_init(self):
       self.total = 0 # for every word in mapper() we will increment this
       self.letter_cache = {} #for every leter that starts a word, we will increment
                              # its entry in the cache
       self.word_cache = {} # for every word, we will update the cache

   def mapper(self, key, line):
       words = line.split()
       for w in words:
           #if key starts with _ it means this is a real word
           wordkey = "_" + w
           self.word_cache[wordkey] = self.word_cache.get(wordkey, 0) + 1
           # this cache could get too big, so we need to check
           if len(self.word_cache) > CACHE_SIZE:
               for k in self.word_cache:
                   yield k, self.word_cache[k]
               self.word_cache.clear() #make sure to clear it, and don't accidentally put this in the for loop

           self.total += 1 # in memory combining, no need to worry about the size
           
           first_letter = w[0].upper() 
           # super important to check this is actually a letter
           # because we just want the letter and this will
           # cause the letter_cache to remain small (at most 26
           # entries)
           if first_letter.isalpha(): 
               letterkey = first_letter + "_" # ends in underscore
               self.letter_cache[letterkey] = self.letter_cache.get(letterkey, 0) + 1

   def mapper_final(self):
       #anything that hasn't been sent yet will be sent now
       yield TOTAL_KEY, self.total
       for letter in self.letter_cache:
           yield letter, self.letter_cache[letter]
       for word in self.word_cache:
           yield   word, self.word_cache[word]


   def reformat_output_key(self, k):
       """ output keys have underscores, we will make them more readable """
       if k == TOTAL_KEY:
           return "[Total Count]"
       elif k.startswith("_"):
           return k[1:] # get rid of first underscore
       else: # it is a letter followed by underscore
           return "[Letter " +  k[0] + " count]"

   def reducer(self, key, valuelist):
       # add up the values, and make the key nicer to look at 
       better_key = self.reformat_output_key(key)
       yield better_key, sum(valuelist)


   def partition(self, key, num_reducers):
       # send all the stats keys (ending in _  to reducer 0, everything else to the other reducers suing hash functions
       thehash = int(hashlib.sha1(bytes(str(key),'utf-8')).hexdigest(), 16) 
       if key.endswith("_"):
           return 0
       else:
           return 1 + (thehash % (num_reducers -1))

if __name__ == "__main__":
    WordStats.run(trace=True)

