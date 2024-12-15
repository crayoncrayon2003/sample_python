from typing import List
from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel, constr

Base = declarative_base()

class UserOrm(Base):
    __tablename__ = 'user'
    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(63), unique=True)
    age  = Column(Integer)
    password1 = Column(ARRAY(String(255)))
    password2 = Column(ARRAY(String(255)))

class UserModel(BaseModel):
    id: int
    name: constr(max_length=63)
    age: int
    password1: constr(max_length=255)
    password2: constr(max_length=255)

    class Config:
        orm_mode = True
        from_attributes=True

def main():
    data = {
        'id': 1,
        'name': 'FirstName FamilyName',
        'age': 30,
        'password1': 'pass',
        'password2': 'pass',
    }

    userA = UserOrm(**data)
    print(userA, type(userA))

    user_model = UserModel.from_orm(userA)
    print(user_model, type(user_model))

if __name__=='__main__':
    main()
