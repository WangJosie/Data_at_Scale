import sys
import os
import MapReduce

mr=MapReduce.MapReduce()



#parse [document_id, text]
def mapper(record):
    seq_id=record[0]
    seq=record[1][:-10]
    mr.emit_intermediate(seq, 1)

def reducer(key, list_of_values):
    mr.emit(key)
    



inputdata=open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)

