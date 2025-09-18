import pytest

# conftest.pyのフィクスチャは、テスト実行時に共通で使える「準備・後片付け」処理を定義するもの
# テスト関数の引数に指定することで、自動的にその値や環境を受け取れます。

@pytest.fixture
def sample_data():
    return {"sample": "data"}