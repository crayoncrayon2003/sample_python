import numpy as np
import cupy as cp  # NumPyの代わりにCuPyを使用
import time

def main():
    # 1,000万個の座標データを作成
    n_points = 10_000_000
    print(f"データ生成中... ({n_points} 点)")
    
    # --- CPU (NumPy) での準備 ---
    data_cpu = np.random.random((n_points, 2)).astype(np.float32)

    # --- GPU (CuPy) での計算 ---
    print("GPUへデータを転送中...")
    t_transfer_start = time.time()
    # データをCPUメモリからGPUメモリへコピー
    data_gpu = cp.asarray(data_cpu)
    t_transfer = time.time() - t_transfer_start

    # GPUでの計算計測
    # ※CuPyは非同期実行されるため、正確な計測には cp.cuda.Device().synchronize() が必要
    cp.cuda.Device().synchronize()
    t0 = time.time()
    
    # 計算ロジック (NumPyとほぼ同じ書き方でOK)
    distances_gpu = cp.sqrt(data_gpu[:, 0]**2 + data_gpu[:, 1]**2)
    
    cp.cuda.Device().synchronize()
    t_gpu = time.time() - t0

    # 結果をCPUに戻す
    distances_cpu = cp.asnumpy(distances_gpu)

    print("-" * 40)
    print(f"GPUデータ転送時間  : {t_transfer:.4f} 秒")
    print(f"GPU純粋計算時間    : {t_gpu:.4f} 秒")
    print(f"合計時間 (転送込)  : {t_transfer + t_gpu:.4f} 秒")
    print("-" * 40)
    print("※注意: 計算内容が単純すぎると、計算速度より転送時間の方が長くなることがあります。")

if __name__ == "__main__":
    main()