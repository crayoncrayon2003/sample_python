import json
import jmespath

data = {
    "people": [
        {"name": "Alice", "age": 30, "city": "Tokyo"},
        {"name": "Bob", "age": 25, "city": "Osaka"},
        {"name": "Carol", "age": 35, "city": "Kyoto"}
    ]
}

query = "people[?age >= `30`].name"
result = jmespath.search(query, data)
print(result)  # ['Alice', 'Carol']

query = "people[].{fullName: name, location: city}"
result = jmespath.search(query, data)
print(result)  # [{'fullName': 'Alice', 'location': 'Tokyo'}, {'fullName': 'Bob', 'location': 'Osaka'}, {'fullName': 'Carol', 'location': 'Kyoto'}]

query = "people[?city == 'Kyoto'].name"
result = jmespath.search(query, data)
print(result)  # ['Carol']


query = """people[].{
    id: name,
    type: 'Person',
    name: {type: 'Text', value: name},
    age: {type: 'Number', value: age},
    city: {type: 'Text', value: city}
}"""

result = jmespath.search(query, data)

print(json.dumps(result, indent=2))