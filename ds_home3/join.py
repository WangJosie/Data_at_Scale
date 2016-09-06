import sys
import os
import MapReduce

mr=MapReduce.MapReduce()

TABLE_1_NAME ="order"
TABLE_2_NAME="line_item"


#parse [document_id, text]
def mapper(record):
    table_name=record[0]
    order_id=record[1]
    table_value=record[2:]
    mr.emit_intermediate(order_id, [table_name, table_value])

def reducer(key, list_of_values):
    result=[] 
    '''
    output is (table_name, order_id, table_value join table_name*, order_id, table_value) tuple
    '''
    table1=[table[1] for table in list_of_values if table[0]==TABLE_1_NAME]
    table2=[table[1] for table in list_of_values if table[0]==TABLE_2_NAME]

    for record1 in table1:
        for record2 in table2: 
            output=[]
            output.append(TABLE_1_NAME)
            output.append(key)
            output.extend(record1)
            output.append(TABLE_2_NAME)
            output.append(key)
            output.extend(record2)
            mr.emit(output)



inputdata=open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)

