'''
Created on Mar 11, 2015

@author: martin.melchior
'''

def flatten_tuple(nested_tuple):
    return tuple(flatten_tuple_to_array(nested_tuple))

        
def flatten_tuple_to_array(nested_tuple):
    result=[]
    if isinstance(nested_tuple, tuple):
        for el in nested_tuple:
            result.extend(flatten_tuple_to_array(el))
        return result
    else:
        return [nested_tuple]  
