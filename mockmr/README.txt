# This is a mapreduce simulator that is similar to the MRJob package
#  however it also allows you to define a custom partitioner and sorter
#  and keeps track of data movement, so that you can see what the inputs and outputs
#  of the mapper are and what the input to the reducer is.
#  The basic code structure looks like the following code below
#    you need to at least define the mapper and reducer function
#    optionally you can define the mapper_init, mapper_final, reducer_init, reducer_final
#    functions. If you do define them, they must yield key, value pairs.
#  You can also define a custom sorter and partioner. The default definitions are included below
#
#  Instructions for running the file are below this code
-----------------------------------
from mockmr import MockMR
import random

class MyMR(MockMR):
    #def mapper_init(self):
    #    pass


    def mapper(self, key, value):
        yield key, value


    #def mapper_final(self):
    #    pass


    #def reducer_init(self):
    #    pass


    def reducer(self, key, values_iterator):
        yield key, sum(values_iterator) 


    #def reducer_final(self):
    #    pass


    #def partition(self, key, num_reducers):
    #    """ This is the default partitioner that you get if you do not implement it 
    #    if you uncomment this function, you may want to import hashlib at the top of the file"""
    #    return int(hashlib.sha1(bytes(str(key),'utf-8')).hexdigest(), 16) % num_reducers


    #def compare(self, a, b):
    #    """ This is the default sorter that you get if you do not implement it.
    #        returns -1 if a<b, 0 if a=b, 1 if a>b """
    #    return (a > b) - (b > a)


if __name__ == "__main__":
    MyMR.run(trace=True) # use this if the input file is not  a key,value file
    # MyMR.run(trace=True, input_kv=True) #if the input file(s) have the format where every line
                                          # is a key followed by a tab followed by a value, then
                                          # uncomment this MyMR.run call, and comment out the run call above it

----------------------------------------------

# To run the code, let's say you put it into a file called tester.py
# 1) if the input you want to use is a single file, you would do this from the command line (not the python shell):
#  python tester.py input=wap.txt output=outputdir mappers=3 reducers=4
#       in place of `wap.txt' you would put in your input file name
#       in place of `outputdir', you would put in the directory where you want the output to go 
#                (don't put / at the end of the output directory)
#       then you set the number of mappers and reducers (you don't always have to pick 3 and 4)
#       IMPORTANT: if the output directory already exists, you would get an error.
#
# 2) if the input you want to use is a directory containing files, you would do this from the command line:
#  python tester.py input=inputdir output=outputdir reducers=4
#       in place of `inputdir' you would put in your input directory name (don't put a / at the end)
#       in place of `outputdir', you would put in the directory where you want the output to go
#                (don't put / at the end of the output directory)
#       then you set the number of reducers (you don't always have to pick  4)
#       IMPORTANT: if the output directory already exists, you would get an error.
#       IMPORTANT: the number of mappers is the number of files you have in inputdir
#
# if you call MyMR.run(trace=True, input_kv=True) then the input must be key-value input (i.e., every
#       line of the input must be a key followed by a tab followed by a value). If the input does not
#       have this format, then call MyMR.run(trace=True) instead
#
#
# After your code finishes running, it will tell you where to find files that have statistics about
#       your job, a file that shows what each mapper's input is, what each mapper's output is,
#       and a file that shows what the input to each reducer is. The output of each reducer
#       will appear in the outputdir that you specified
