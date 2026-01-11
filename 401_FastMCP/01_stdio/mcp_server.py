"""
MCP Server for Key-Value Store
FastMCPを使用したMCPサーバの実装例
"""
import httpx
from mcp.server.fastmcp import FastMCP

# MCPサーバの初期化
mcp = FastMCP("store-server")

# REST APIのベースURL（ポート8001に変更）
API_BASE_URL = "http://localhost:8001"

@mcp.tool()
async def store_set(key: str, value: str) -> str:
    """
    キーと値をストアに保存します

    Args:
        key: 保存するキー
        value: 保存する値

    Returns:
        保存結果のメッセージ
    """
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{API_BASE_URL}/store/set",
            json={"key": key, "value": value}
        )
        result = response.json()
        return f"✅ キー '{key}' に値 '{value}' を保存しました。ステータス: {result['status']}"

@mcp.tool()
async def store_get(key: str) -> str:
    """
    キーに対応する値をストアから取得します

    Args:
        key: 取得したいキー

    Returns:
        取得した値、またはキーが存在しない場合のメッセージ
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{API_BASE_URL}/store/get",
            params={"key": key}
        )
        result = response.json()

        if result["value"] is None:
            return f"❌ キー '{key}' は存在しません"
        else:
            return f"📦 キー '{key}' の値: {result['value']}"

@mcp.resource("store://status")
def get_store_status() -> str:
    """ストアサーバの状態を返す"""
    return f"Store Server Status: Connected to {API_BASE_URL}"

if __name__ == "__main__":
    mcp.run(transport="stdio")