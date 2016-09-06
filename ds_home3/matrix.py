import sys
import os
import MapReduce

mr =MapReduce.MapReduce()

#matrix A is M*K
#matrix b is K*N

M=5
K=5
N=5



#parse [matrix_label, row, colum, value]
def mapper(record):
    matrix, row, col, value=record
    for n in range(N):
      if matrix=="a":
          destination_cell=(row, n)
          matrix_position ='L'
          serial_num=col
      else:
          destination_cell=(n, col)
          matrix_position='R'
          serial_num=row
      mr.emit_intermediate(destination_cell,(matrix_position, serial_num, value))

def reducer(key, list_of_values):
    
    '''
    '''

    left_matrix_value=[(item[1], item[2]) for item in list_of_values if item[0] =='L']
    right_matrix_value=[(item[1], item[2] )for item in list_of_values if item[0]=='R']
    result=0
      
    for item_L in left_matrix_value:
         for item_R in right_matrix_value:
             if item_L[0] == item_R[0]:
                #print item_L[1], item_R[1]
                result += item_L[1]*item_R[1]

    mr.emit((key[0], key[1], result))


inputdata=open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)

