import json
from jsonpath_ng import parse

data = {
    "people": [
        {"name": "Alice", "age": 30, "city": "Tokyo"},
        {"name": "Bob", "age": 25, "city": "Osaka"},
        {"name": "Carol", "age": 35, "city": "Kyoto"}
    ]
}

jsonpath_expr = parse("$.people[*]")
result = [match.value for match in jsonpath_expr.find(data)]

filtered_names = [person["name"] for person in result if person["age"] >= 30]
print(filtered_names)  # ['Alice', 'Carol']

filtered_data = [{"fullName": person["name"], "location": person["city"]} for person in result]
print(filtered_data)  # [{'fullName': 'Alice', 'location': 'Tokyo'}, {'fullName': 'Bob', 'location': 'Osaka'}, {'fullName': 'Carol', 'location': 'Kyoto'}]

kyoto_people = [person["name"] for person in result if person["city"] == "Kyoto"]
print(kyoto_people)  # ['Carol']


ngsi_result = []
for person in result:
    transformed = {
        "id": person["name"],
        "type": "Person",
        "name": {"type": "Text", "value": person["name"]},
        "age": {"type": "Number", "value": person["age"]},
        "city": {"type": "Text", "value": person["city"]}
    }
    ngsi_result.append(transformed)

print(json.dumps(ngsi_result, indent=2))
