import sys
import os
import math
import functools
import traceback
import itertools
import hashlib

class MockMR:

    # override these

    def mapper_init(self):
        pass


    def mapper(self, key, value):
        pass


    def mapper_final(self):
        pass


    def reducer_init(self):
        pass


    def reducer(self, key, values_iterator):
        pass


    def reducer_final(self):
        pass


    def partition(self, key, num_reducers):
        return int(hashlib.sha1(bytes(str(key),'utf-8')).hexdigest(), 16) % num_reducers


    def compare(self, a, b):
        """ returns -1 if a<b, 0 if a=b, 1 if a>b """
        return (a > b) - (b > a)

    # variables and methods needed for MR Simulation
    _INPUT = "input"
    _OUTPUT = "output"
    _INFO = "info" # prefix for file names about job statistics
    _MAPPERS = "mappers"
    _REDUCERS = "reducers"
    _STATS = "stats" # identifies file with job statistics
    _MAPIN = "map-in" # identifies file with mapper input informaiton
    _MAPOUT = "map-out" #identifies file with mapper output information
    _REDIN = "red-in" #identifies file with reducer input information


    #instance variables:
    #  self._stats_file : name of the file containing job statistics 
    #  self._map_in_file : name of the file containing the inputs to each mapper
    #  self._map_out_file : name of the file containing mapper outputs
    #  self._red_in_file : name of the file containing the reducer inputs
    #  self._input_file_name : name of the input file, currently only one input file supported
    #  self._output_dir : name of directory to place outputs
    #  self._num_mappers : number of mappers
    #  self._num_reducers : number of reducers
    #  self._input_dir: the input directory if file is not given
    #  self._output_files : a list of output file objects
    #  self._open_files : a list of open files
    #  self._trace : whether to track statistics or not
   

    def __init__(self):
        self._open_files = []
        self._output_files = []
        self._input_file_name = None
        self._input_dir_name = None


    @classmethod
    def run(cls, trace=False, input_kv=False):
        """ Runs the MR simulator. If trace is true, statistics are collected
        if input_kv is true, input is treated as tab-separate key, value pairs
        otherwise input is just a text file with key=None and value= the line
        """
        mapin = None   #mapper_inputs is a list of lists of key value pairs, 
                       #mapin[i][j] is the jth key value pair given to mapper i

        mapout = None  #The mapper output is a list of lists of lists of key value pairs. 
                       #mapout[i][j] is the list of key value pairs output by mapper i 
                       # for its jth line of input. 
        redin = None
        redout = None
        mapstatus = False # status of the mapper job, whether failed or not
        shuffle_status = False # status of the shuffle and sort phase, whether it failed or not
        redstatus = False # status of the reducer phase
        with cls() as mr:
            mr._set_trace(trace) # are we keeping track of runtime statistics?
            status = mr._read_args() # read in command line arguments
            if not status: #check if errors happened
                return
            mr._set_stats_files_names() # name the files that will store the job statistics
            status = mr._ensure_output_dir() # make sure the output file diretory exists, and create it if it doesn't.
            if not status: # check if errors happened
                return
            raw_mr_input = mr._open_the_files_and_read_input() # open the input and output files
            mapout, mapin, mapstatus = mr._map_phase(cls, raw_mr_input, input_kv=input_kv) #get mapper input and mapper output
            if mapstatus:
                redin, shuffle_status = mr._shuffle_sort(mapout)
            if shuffle_status:
                redout, redstatus = mr._reduce_phase(cls, redin)
            if redstatus:
                mr._write_output(redout)
                mr._print_stats(mapin, mapout, redin, redout)
                print("Hooray, your job completed.")
                print(f"  The output is in the directory `{mr._output_dir}'")
                print(f"  Job statistics are in the file `{mr._stats_file}'")
                if mr._trace:
                    print(f"  Mapper input info is in `{mr._map_in_file}'")
                    print(f"  Mapper output info is in `{mr._map_out_file}'")
                    print(f"  Reducer input info is in `{mr._red_in_file}'")


    def _print_stats(self, mapin, mapout, redin, redout):
        # overall stats
        with open(self._stats_file, "w") as f_stats:
            mapper_input_sizes = [len(x) for x in mapin]
            total_mapper_input = sum(mapper_input_sizes)
            mapper_output_sizes = [sum(len(y) for y in x) for x in mapout]
            total_mapper_output = sum(mapper_output_sizes)
            f_stats.write(f"Map Phase, # input lines: {total_mapper_input}, # output (k,v) pairs: {total_mapper_output} {os.linesep}")
            for i in range(self._num_mappers):
                f_stats.write(f"    Mapper {i}, # input lines: {mapper_input_sizes[i]}, # output (k,v) pairs: {mapper_output_sizes[i]} {os.linesep}")
            reducer_input_sizes = [len(x) for x in redin]
            total_reducer_input = sum(reducer_input_sizes)
            reducer_output_sizes = [len(x) for x in redout]
            total_reducer_output = sum(reducer_output_sizes)
            f_stats.write(f"Reduce Phase, # input keys: {total_reducer_input}, # output (k,v) pairs: {total_reducer_output} {os.linesep}")
            for i in range(self._num_reducers):
                f_stats.write(f"    Reducer {i}, # input keys: {reducer_input_sizes[i]}, # output (k,v) pairs: {reducer_output_sizes[i]} {os.linesep}")
        if not self._trace:
            return
        #  inputs to each mapper   
        with open(self._map_in_file, "w") as f_trace:
            f_trace.write(f"### These are the inputs to the mappers {os.linesep}")
            for (i, per_mapper_input) in enumerate(mapin):
                f_trace.write(f"Mapper {i}: {os.linesep}")
                for thing in per_mapper_input:
                    f_trace.write(f"    {str(thing)}{os.linesep}")
        # mapper outputs
        mysep = "-----"
        with open(self._map_out_file, "w") as f_trace:
            f_trace.write(f"### These are the (k,v) pairs produced by mappers {os.linesep}")
            for (i, per_mapper_output) in enumerate(mapout):
                f_trace.write(f"Mapper {i}: {os.linesep} (output k,v pairs resulting from different input lines are separated by {mysep}) {os.linesep}")
                f_trace.write(f"    {mysep}{os.linesep}")
                for line_no, per_line_output in enumerate(per_mapper_output):
                    for thing in per_line_output:
                        f_trace.write(f"    {str(thing)}{os.linesep}")
                    f_trace.write(f"    {mysep}{os.linesep}")

        #  inputs to each reducer   
        with open(self._red_in_file, "w") as f_trace:
            f_trace.write(f"### These are the inputs to the reducers {os.linesep}")
            for (i, per_reducer_input) in enumerate(redin):
                f_trace.write(f"Reducer {i}: {os.linesep}")
                for thing in per_reducer_input:
                    f_trace.write(f"    {str(thing)}{os.linesep}")

   

    def _write_output(self, redout):
        """ writes the output of the reduce phase into one file per reducer
        input is a list of list of k,v pairs where redout[i] is the list of k,v pairs produced by reducer i"""
        for thefile, per_reducer_output in zip(self._output_files, redout):
            for k,v in per_reducer_output:
                thefile.write(f"{k}\t{v}{os.linesep}")


    def _line_parser(self, line, input_kv=False):
        """ parses a line into key, value pairs
        if input_kv is true, then the line must have a key followed by a tab followed by a value
        if input_kv is false, key is None, value is the line
        """
        if input_kv:
            parts = line.split("\t", 1)
            if len(parts) != 2:
                raise Exception("Input line: `{line}' does not have key and value separated by a tab, but input_kv is set to True")
            return (parts[0], parts[1])
        else:
            return (None, line) 


    def _map_phase(self, cls, raw_mr_input, input_kv=False):
        """ Read input, assign to different mappers, and perform map phase, returning
        input to each mapper and output of each mapper 
        The mapper output is a list of lists of lists of key value pairs. mapper_output[i][j] is the list
        of key value pairs output by mapper i for its jth line of input. mapper_inputs has is a list of lists
        of key value pairs, where mapper_inputs[i][j] is the jth key value pair given to mapper i
        """
        map_ok = True # whether map phase is going well so far

        mapper_outputs = [[] for _ in range(self._num_mappers)]
        mapper_inputs = [[] for _ in range(self._num_mappers)]
        the_mappers = [cls() for _ in range(self._num_mappers)] # list of mappers
        
        #mapper init
        for i in range(self._num_mappers):
            mygen = the_mappers[i].mapper_init()
            output_kv = list(mygen) if mygen is not None else []
            if not self._check_kv_list(output_kv): # check that mapper is producing key value pairs
                map_ok = False
                print(f"Mapper_init produced an output that is not a key value pair: {str(output_kv)}")
            else:
                if len(output_kv) > 0:
                    mapper_outputs[i].append(output_kv)
        #mapper
        for mapper_id, input_lines in enumerate(raw_mr_input):
            for line in input_lines:
                (k, v) = self._line_parser(line, input_kv=input_kv)
                mapper_inputs[mapper_id].append((k,v))
                mygen = the_mappers[mapper_id].mapper(k,v) #mapper call creates a generator
                output_kv = list(mygen) if mygen is not None else [] # this gives the list of key value pairs of the most recent mapper call
                if not self._check_kv_list(output_kv): # check that mapper is producing key value pairs
                    map_ok = False
                    print(f"Mapper did not output key value pair for in line: `{line}'")
                else:
                    if len(output_kv) > 0:
                        mapper_outputs[mapper_id].append(output_kv)
        # mapper final
        for i in range(self._num_mappers):
            mygen = the_mappers[i].mapper_final()
            output_kv = list(mygen) if mygen is not None else []
            if not self._check_kv_list(output_kv): # check that mapper is producing key value pairs
                map_ok = False
                print(f"Mapper_final produced an output that is not a key value pair: {str(output_kv)}")
            else:
                if len(output_kv) > 0:
                    mapper_outputs[i].append(output_kv) 

        return mapper_outputs, mapper_inputs, map_ok
        

    def _shuffle_sort(self, mapout):
        """ performs the suffle and sort phase on the mapout, the outputs of the mappers
        mapout is organized so that mapout[i][j] is the list of key value pairs output by mapper
        i for its jth input. It procedues the reducer_input variable, which is a list such that
        reducer_input[i][j] is the jth (k, valuelist) that reducer i sees """
        isok = True #status of the shuffle and sort phase
        reducer_received = [[] for _ in range(self._num_reducers)] # the ith entry is the list of (k,v) sent to reducer i
                                                                   # these will need to be grouped and sorted
        # shuffle phase
        # first collect k,v pairs sent to reducers
        for per_mapper_outputs in mapout:
            for per_line_outputs in per_mapper_outputs:
                for (k,v) in per_line_outputs:
                    redid = self.partition(k, self._num_reducers) # get the reducer for this k,v pair
                    mycheck = self._check_partitioner(redid) # make sure the reducer id is valid
                    if not mycheck:
                        print(f"On key: `{k}', partition did not return an integer between 0 and {self._num_reducers - 1}")
                        isok = False
                        return [], isok
                    reducer_received[redid].append((k,v))
        # now sort input to each reducer
        for received in reducer_received:
            received.sort(key=functools.cmp_to_key(lambda a,b: self.compare(a[0],b[0])))
        # now group together values with same key
        reducer_input = [
               [ (grouped_kv[0], [thing[1] for thing in grouped_kv[1]]) # create key / value list
                  for grouped_kv in itertools.groupby(inp, key=lambda x: x[0]) # group consecutive keys together
                ] for inp in reducer_received  # for every reducer
            ]
        sortcheck = self._check_good_sort(reducer_input) # check that the sorter did not have obvious errors,
                                                         # which would happen if the same key did get sorted consecutively
        if not sortcheck:
            isok = False
            return [], isok
        return (reducer_input, isok)

    def _check_partitioner(self, k):
        """ checks that k is a valid id for a reducer """
        if type(k) != type(1) or k < 0 or k >= self._num_reducers:
            return False
        return True

    def _reduce_phase(self, cls, redin):
        """ performs the reduce phase. redin is a list of lists of (k, vlist) pairs,
        where redin[i][j] is the jth input to reducer i. Produces redout such that redout[i] is the list of (k,v)
        pairs output by the reducer """
        isok = True
        redout = [[] for _ in range(self._num_reducers)]
        the_reducers = [cls() for _ in redin] # create the reducers
        # reducer_init
        for i in range(self._num_reducers):
            mygen = the_reducers[i].reducer_init()
            output_kv = list(mygen) if mygen is not None else []
            if not self._check_kv_list(output_kv): # check that mapper is producing key value pairs
                isok = False
                print(f"reducer_init produced an output that is not a key value pair: {str(output_kv)}")
            else:
                if len(output_kv) > 0:
                    redout[i].extend(output_kv)
        # reducer
        for reducer_index, per_reducer_input in enumerate(redin):
            local_output = []
            for k,vlist in per_reducer_input:
                mygen = the_reducers[reducer_index].reducer(k,vlist) #reducer call creates a generator
                output_kv = list(mygen) if mygen is not None else []
                if not self._check_kv_list(output_kv): # check that mapper is producing key value pairs
                    isok = False
                    print(f"Reducer did not output only key value pairs when its input is key: {k} and valuelist {str(vlist)}")
                if len(output_kv) > 0:
                    redout[reducer_index].extend(output_kv)
        #reducer_final
        # reducer_init
        for i in range(self._num_reducers):
            mygen = the_reducers[i].reducer_final()
            output_kv = list(mygen) if mygen is not None else []
            if not self._check_kv_list(output_kv): # check that mapper is producing key value pairs
                isok = False
                print(f"reducer_finale produced an output that is not a key value pair: {str(output_kv)}")
            else:
                if len(output_kv) > 0:
                    redout[i].extend(output_kv)
        return redout, isok
         

    def _check_good_sort(self, reducer_input):
        """ checks for obvious sorter/partitioner errors where the same key appears
        in multiple reducers (partition error) or in multiplie places in the input
        to the same reducer (so that after sorting with the sorter, the same key did not appear
        consecutively and hence was not grouped together) """
        isok=True
        keydict = {} # stores key and which reducer it got it from
        for index, per_reducer_input in enumerate(reducer_input):
            for (k, vlist) in per_reducer_input:
                if k in keydict:
                    isok = False
                    if index == keydict[k]:
                        print(f"There is an error with your sorter. It does not follow the rules for how a sorter should operate. This was detected in Reducer {index}.")
                    else:
                        print(f"Reducers {index} and {keydict[k]} received the same key {k}. This is a problem with your partitioner.")
                keydict[k]=index
        return isok

    def _check_kv_list(self, kvlist):
        """ check that kvlist is a list of key, value tuples"""
        retvalue = True
        for thing in kvlist:
            if type(thing) != type((1,2)) or len(thing) != 2: #check that it is a (k,v) pair
                retvalue = False                
        return retvalue
        

    def _set_trace(self, trace):
        """ Whether to keep track of runtime statistics """
        self._trace=trace


    def _open_the_files_and_read_input(self):
        """ open all the necessary input, output  """
        for i in range(self._num_reducers):
            outfilename = f"{self._output_dir}/part-{str(i).rjust(4, '0')}"
            thefile = open(outfilename, "w")
            self._output_files.append(thefile)
            self._open_files.append(thefile)
 
        raw_input_lines = [[] for _ in range(self._num_mappers)] # the ith element is the list of lines for mapper i
        if self._input_file_name is not None:
            with open(self._input_file_name, "r") as input_file:
                input_lines = [line.rstrip("\n\r") for line in input_file.readlines() if len(line.strip()) > 0]
            total_lines = len(input_lines)
            increment = int(math.ceil(total_lines / self._num_mappers)) # max number of lines handled by a mapper
            lines_left = increment
            mapper_id = 0
            line_number = 0
            for line in input_lines:
                if lines_left == 0: # start of a new mapper
                    lines_left = increment
                    mapper_id = mapper_id + 1
                raw_input_lines[mapper_id].append(line)
                lines_left = lines_left - 1
                line_number = line_number + 1
        else:
            files_in_dir = sorted([x for x in os.listdir(self._input_dir_name) if os.path.isfile(f"{self._input_dir_name}/{x}")])
            for (i, f) in enumerate(files_in_dir):
                with open(f"{self._input_dir_name}/{f}", "r") as input_file:
                    input_lines = [line.rstrip("\n\r") for line in input_file.readlines()  if len(line.strip()) > 0]
                raw_input_lines[i] = input_lines
        return raw_input_lines

    def _print_usage(self):
        """ Tells user how to call the python script """
        print(f"Usage: python {sys.argv[0]} {self._INPUT}=inputfile {self._OUTPUT}=outputdir {self._MAPPERS}=nummappers {self._REDUCERS}=numreducers")
        print("    or    ")
        print(f"python {sys.argv[0]} {self._INPUT}=inputdir {self._OUTPUT}=outputdir {self._REDUCERS}=numreducers")
        print(" ")
        print(" if the input is a file, you need to specify the number of mappers.")
        print(" if the input is a directory, the number of mappers will be set to the number of files in the directory")


    def __enter__(self):
        """ enables `with' usage for this class"""
        return self


    def __exit__(self,  exc_type, exc_value, tb):
        """ cleanup after 'with' ends """
        for f in self._open_files:
            f.close()
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
             


    def _ensure_output_dir(self):
        """ Makes sure that the desired output directory exists and is empty """
        fail = False
        if os.path.exists(self._output_dir):
            if not os.path.isdir(self._output_dir):
                print(f"ERROR: Specified output directory '{self._output_dir}' exists and is not a directory")
                fail = True
            elif len(os.listdir(self._output_dir)) > 0:
                print(f"ERROR: Specified output directory '{self._output_dir}' is not empty!")
                fail = True
        else:
            try:
                os.mkdir(self._output_dir)
            except:
                print(f"ERROR: Cannot create output directory '{self._output_dir}'")
                fail=True
        return not fail


    def _read_args(self):
        """ Gets and checks the command line arguments """
        required = [self._INPUT, self._OUTPUT, self._MAPPERS, self._REDUCERS]
        myargs = {}
        error = ""
        self._num_mappers = -1
        self._num_reducers = -1
        is_input_a_dir = False
        try:
            for arg in sys.argv[1:]:
                parts = arg.split("=", 1)
                if len(parts) ==2:
                    myargs[parts[0]] = parts[1]
            if self._INPUT in myargs:
                if os.path.isdir(myargs[self._INPUT]): #if we are given an input directory, find the number of files it has and set mappers to that
                    files_in_dir = [x for x in os.listdir(myargs[self._INPUT]) if os.path.isfile(f"{myargs[self._INPUT]}/{x}")]
                    myargs[self._MAPPERS] = len(files_in_dir)
                    is_input_a_dir = True
            for req in required:
                if req not in myargs:
                    error = f"'{req}' is not specified"
                    raise Exception()
            if is_input_a_dir:
                self._input_dir_name = myargs[self._INPUT]
            else:
                self._input_file_name = myargs[self._INPUT]
            self._output_dir = myargs[self._OUTPUT]
            self._num_mappers = int(myargs[self._MAPPERS])
            self._num_reducers = int(myargs[self._REDUCERS])
            assert self._num_mappers > 0
            assert self._num_reducers > 0
        except:
            if error == "":
                if self._num_mappers < 1:
                    print("Number of mappers must be a positive integer")
                if self._num_reducers < 1:
                    print("Number of reducers must be a positive integer")
            else:
                print(error)
            self._print_usage()
            return False
        return True

    def _set_stats_files_names(self):
        """ set the name of the files for job statistics, mapper input, mapper output, reducer input"""
        basename = self.__class__.__name__
        self._stats_file = f"{self._INFO}_{basename}_{self._STATS}.txt"
        self._map_in_file = f"{self._INFO}_{basename}_{self._MAPIN}.txt"
        self._map_out_file = f"{self._INFO}_{basename}_{self._MAPOUT}.txt"
        self._red_in_file = f"{self._INFO}_{basename}_{self._REDIN}.txt"
    
