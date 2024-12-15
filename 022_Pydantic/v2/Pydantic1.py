from pydantic import BaseModel

class UserModel(BaseModel):
    id  : int
    name: str
    age : int
    password1: str
    password2: str


def main():
    data = {
        'id': 1,
        'name': 'FirstName FamilyName',
        'age': 30,
        'password1': 'pass',
        'password2': 'pass',
    }

    userA = UserModel(**data)
    print(userA)

if __name__=='__main__':
    main()
