def getHello():
    return "hello\n", 200

data={}

def getData(id):
    # Checking input parameter
    if id is None:
        return {"message": "key error", "id": id}, 404

    if id not in data:
        return {"message": "id not found", "id": id}, 404

    return {"id": id, "value": data[id]}, 200


def postData(body):
    # Checking input parameter
    required_keys = {"id", "value"}
    if not required_keys.issubset(body.keys()):
        return {"message": "key error", "data": body}, 404

    # Extracting input parameters
    id    = int(body['id'])
    value = body['value']

    # data registration
    data[id] = value
    return {"message": "Data created", "id": id, "value": value}, 200
