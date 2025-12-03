import strawberry
from db_postgres import get_user
from db_mongo import get_orders
from external_rest import get_fixed_data

# User 型
@strawberry.type
class User:
    name: str

# Order 型（MongoDB のキーに合わせて修正）
@strawberry.type
class Order:
    item: str
    quantity: int

# FixedAPI 型
@strawberry.type
class FixedAPI:
    service: str
    value: int

# Summary 型
@strawberry.type
class Summary:
    user: User
    orders: list[Order]
    fixed_api: FixedAPI

# Query 型
@strawberry.type
class Query:
    @strawberry.field
    def summary(self) -> Summary:
        user_data = get_user()
        orders_data = get_orders()
        fixed_data = get_fixed_data(key="default")

        return Summary(
            user=User(**user_data),
            orders=[Order(**o) for o in orders_data],
            fixed_api=FixedAPI(service=fixed_data["service"], value=fixed_data["value"])
        )

# スキーマ定義
schema = strawberry.Schema(query=Query)
