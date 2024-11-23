from db.user import CO
from model.user import UserModel
from bson import ObjectId
import json

# Custom encoder
class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

# get user
def get_user():
    users = []
    for user in CO.find():
        users.append(json.loads(json.dumps(user, cls=JSONEncoder)))
    return users

# set user
def set_user(user: UserModel):
    # Check id
    if hasattr(user, 'id'):
        # del user id
        delattr(user, 'id')

    CO.insert_one(user.model_dump(by_alias=True))

    return get_user()

# del user
def delete_user(index: str):
    CO.delete_one( {'_id': ObjectId(index)} )

    return get_user()
