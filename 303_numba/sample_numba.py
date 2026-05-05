import numpy as np
import time
from numba import njit, prange

# --- 計算ロジック ---

def calculate_distances_python(points):
    """通常のPython（NumPy配列へのループ）"""
    n = len(points)
    distances = np.empty(n)
    for i in range(n):
        distances[i] = np.sqrt(points[i, 0]**2 + points[i, 1]**2)
    return distances

@njit
def calculate_distances_numba(points):
    """NumbaによるJITコンパイル"""
    n = len(points)
    distances = np.empty(n)
    for i in range(n):
        distances[i] = np.sqrt(points[i, 0]**2 + points[i, 1]**2)
    return distances

@njit(parallel=True)
def calculate_distances_numba_parallel(points):
    """Numbaによる並列処理 (マルチコア活用)"""
    n = len(points)
    distances = np.empty(n)
    # 並列化する場合は prange を使用
    for i in prange(n):
        distances[i] = np.sqrt(points[i, 0]**2 + points[i, 1]**2)
    return distances

# --- メイン処理 ---

def main():
    # 1,000万個の座標データを作成
    n_points = 10_000_000
    print(f"データ生成中... ({n_points} 点)")
    data = np.random.random((n_points, 2)).astype(np.float64)
    
    print("-" * 40)

    # 1. 標準Python
    t0 = time.time()
    calculate_distances_python(data)
    t_py = time.time() - t0
    print(f"1. 標準Python          : {t_py:.4f} 秒")

    # 2. Numba (初回: コンパイル含む)
    t0 = time.time()
    calculate_distances_numba(data)
    t_numba_1st = time.time() - t0
    print(f"2. Numba (初回実行)    : {t_numba_1st:.4f} 秒 (コンパイル含む)")

    # 3. Numba (2回目: コンパイル済み)
    t0 = time.time()
    calculate_distances_numba(data)
    t_numba_2nd = time.time() - t0
    print(f"3. Numba (2回目以降)   : {t_numba_2nd:.4f} 秒")

    # 4. Numba (並列処理)
    # ※並列化用のコンパイルが走るため、2回計測
    calculate_distances_numba_parallel(data) # warm up
    t0 = time.time()
    calculate_distances_numba_parallel(data)
    t_numba_parallel = time.time() - t0
    print(f"4. Numba (並列処理)    : {t_numba_parallel:.4f} 秒")

    print("-" * 40)
    print(f"結論: Numba(並列)は標準Pythonより 約 {t_py / t_numba_parallel:.1f} 倍速くなりました。")

if __name__ == "__main__":
    main()