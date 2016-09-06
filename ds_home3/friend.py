import sys
import os
import MapReduce

mr=MapReduce.MapReduce()



#parse [document_id, text]
def mapper(record):
    person=record[0]
    friend=record[1]
    mr.emit_intermediate(person, friend)

def reducer(key, list_of_values):
    mr.emit((key, len(list_of_values)))
    



inputdata=open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)

