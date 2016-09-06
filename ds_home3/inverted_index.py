import sys
import os
import MapReduce

mr =MapReduce.MapReduce()


#parse [document_id, text]
def mapper(record):
    key=record[0]
    value=record[1]
    for w in value.split():    #further split text
        mr.emit_intermediate(w,key) 

def reducer(key, list_of_values):
    result=[]
    '''
    output is (word, document ID list) tuple
    '''
    for document_id in list_of_values:
        if document_id not in result:
            result.append(document_id)
    mr.emit((key,result))

inputdata=open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)

