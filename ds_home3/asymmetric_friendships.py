import sys
import os
import MapReduce

mr=MapReduce.MapReduce()



#parse [document_id, text]
def mapper(record):
    friend_pair=tuple(sorted(record))
    mr.emit_intermediate(friend_pair, 1)

def reducer(friend_pair, list_of_values):
    if len(list_of_values) ==1:
        mr.emit((friend_pair[0], friend_pair[1]))
        mr.emit((friend_pair[1], friend_pair[0]))
    



inputdata=open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)

