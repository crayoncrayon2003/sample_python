"""
MCP Client Test Script (stdio Transport)
stdioトランスポートを使用したMCPサーバをテストするクライアント
"""
import os
import asyncio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

ROOT   = os.path.dirname(os.path.abspath(__file__))

async def test_mcp_server():
    """MCPサーバのツールをテストする（stdio経由）"""

    # サーバパラメータの設定
    server_params = StdioServerParameters(
        command="python",
        args=[os.path.join(ROOT, "mcp_server.py")],
        env=None
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            # サーバの初期化
            await session.initialize()

            print("=== MCPサーバに接続しました (stdio) ===\n")

            # 1. 利用可能なツールのリストを取得
            print("📋 利用可能なツール一覧:")
            tools = await session.list_tools()
            for tool in tools.tools:
                print(f"  - {tool.name}: {tool.description}")
            print()

            # 2. store_set ツールを呼び出し
            print("🔧 テスト1: store_set ツールを呼び出し")
            result = await session.call_tool("store_set", arguments={
                "key": "username",
                "value": "Alice"
            })
            print(f"結果: {result.content[0].text}\n")

            # 3. store_get ツールを呼び出し
            print("🔧 テスト2: store_get ツールを呼び出し")
            result = await session.call_tool("store_get", arguments={
                "key": "username"
            })
            print(f"結果: {result.content[0].text}\n")

            # 4. 複数のデータを保存
            print("🔧 テスト3: 複数のデータを保存")
            test_data = [
                ("email", "alice@example.com"),
                ("age", "30"),
                ("city", "Tokyo")
            ]
            for key, value in test_data:
                result = await session.call_tool("store_set", arguments={
                    "key": key,
                    "value": value
                })
                print(f"  {result.content[0].text}")
            print()

            # 5. 保存したデータを取得
            print("🔧 テスト4: 保存したデータを取得")
            for key, _ in test_data:
                result = await session.call_tool("store_get", arguments={
                    "key": key
                })
                print(f"  {result.content[0].text}")
            print()

            # 6. 存在しないキーを取得
            print("🔧 テスト5: 存在しないキーを取得")
            result = await session.call_tool("store_get", arguments={
                "key": "nonexistent"
            })
            print(f"結果: {result.content[0].text}\n")

            # 7. リソースの取得（オプション）
            print("📦 利用可能なリソース:")
            try:
                resources = await session.list_resources()
                for resource in resources.resources:
                    print(f"  - {resource.uri}: {resource.name}")
                    # リソースの内容を読み取る
                    content = await session.read_resource(resource.uri)
                    print(f"    内容: {content.contents[0].text}")
            except Exception as e:
                print(f"  リソース取得エラー: {e}")

            print("\n=== テスト完了 ===")

if __name__ == "__main__":
    asyncio.run(test_mcp_server())