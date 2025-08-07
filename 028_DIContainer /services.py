from abc import ABC, abstractmethod

# 1. 依存される側のインターフェース (抽象基底クラス)
#    「通知を送る」という機能の仕様だけを定義
class MyService(ABC):
    @abstractmethod
    def send(self, recipient: str, message: str):
        """指定された宛先にメッセージを送信する"""
        pass

# 2. NotificationServiceの具体的な実装クラス
class MyServiceEmail(MyService):
    def send(self, recipient: str, message: str):
        print(f"Sent Email : {recipient}: {message}")

class MyServiceSMS(MyService):
    """SMSで通知を送るサービス"""
    def send(self, recipient: str, message: str):
        print(f"Sent SMS   : {recipient}: {message}")

# 3. 依存する側のクラス (利用者)
class UserService:
    def __init__(self, myService: MyService):
        self._myService = myService
        print(f"UserService has been created with {myService.__class__.__name__}")

    def register_user(self, username: str):
        print(f"User '{username}' registered.")
        self._myService.send(
            recipient=f"{username}@sample.com",
            message=f"Welcome aboard, {username}!"
        )