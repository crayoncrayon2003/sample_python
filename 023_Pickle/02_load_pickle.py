import os
import pickle
import json

ROOT = os.path.dirname(os.path.abspath(__file__))
FILE = os.path.join(ROOT,'test.pickle')

# load object
with open(FILE, 'rb') as f:
    test_data = pickle.load(f)

print(json.dumps(test_data, indent=2))