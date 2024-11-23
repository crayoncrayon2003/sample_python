
from fastapi import APIRouter
import controllers.user as ctrl
from model.user import UserModel

router = APIRouter()

# GETの処理 全indexで取得
@router.get('/users')
def get_users():
    return {'users': ctrl.get_user()}

# POSTの処理 １ユーザ追加
@router.post('/users')
def set_user(user: UserModel):
    return {'users': ctrl.set_user(user)}

# PUTの処理 キー：userの値を削除
@router.delete('/users/{index}')
def delete_user(index: str):
    return {'users': ctrl.delete_user(index)}