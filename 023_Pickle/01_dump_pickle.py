import os
import pickle
import json

ROOT = os.path.dirname(os.path.abspath(__file__))
FILE = os.path.join(ROOT,'test.pickle')

test_data = {
    "key1" : "value1",
    "key2" : "value2",
    "key3" : "value3",
}

print(json.dumps(test_data, indent=2))

# dump object
with open(FILE, 'wb') as f:
    pickle.dump(test_data, f)