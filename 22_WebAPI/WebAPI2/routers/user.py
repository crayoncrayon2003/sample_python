from fastapi import APIRouter, Body

router = APIRouter()

user_list = ['John', 'Bill', 'Eric']

# GET user
@router.get('/users')
def get_users():
    return {'users': user_list}

# GET user with id
@router.get('/users/{index}')
def get_user(index: int):
    return {'user': user_list[index]}

# add user
@router.post('/users')
def create_user(body=Body(...)):
    user = body.get('user')
    user_list.append(user)
    return {}

# change user with id
@router.put('/users/{index}', )
def update_user(index: int, body=Body(...)):
    user = body.get('user')
    user_list[index] = user
    return {}

# del user
@router.delete('/users/{index}')
def delete_user(index: int):
    user_list.pop(index)
    return {}