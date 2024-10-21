import os
import pickle
import json

# dict to str
dict_data1 = {
    "key1" : "value1",
    "key2" : "value2",
    "key3" : "value3",
}

str_data = json.dumps(dict_data1, indent=2)
print("value : ", str_data)
print("type  : ", type(str_data))

# str to dict
dict_data2 = json.loads(str_data)
print("value : ", dict_data2)
print("type  : ", type(dict_data2))
