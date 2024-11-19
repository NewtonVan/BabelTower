import pandas as pd
import os

def read_benchmark_results(file_path: str) -> pd.DataFrame:
    """读取基准测试结果文件并返回DataFrame"""
    try:
        df = pd.read_csv(file_path)
    except pd.errors.ParserError:
        with open(file_path, 'r') as file:
            lines = file.readlines()
            columns = lines[0].strip().split(',')
            data = [line.strip().split(',') for line in lines[1:]]
            df = pd.DataFrame(data, columns=columns)
    return df

def save_benchmark_results(df: pd.DataFrame, output_dir: str) -> None:
    """保存DataFrame到指定目录"""
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'analysis.csv')
    df.to_csv(output_file, index=False)

def calculate_statistics(df: pd.DataFrame) -> None:
    """计算并打印tx列的统计数据"""
    df['tx'] = pd.to_numeric(df['tx'])
    mean_tx = df['tx'].mean()
    p95_tx = df['tx'].quantile(1-0.95)
    p99_tx = df['tx'].quantile(1-0.99)
    print(f"Mean of tx: {mean_tx}")
    print(f"95th percentile of tx: {p95_tx}")
    print(f"99th percentile of tx: {p99_tx}")

def main() -> None:
    file_path = 'bench_result/result'
    output_dir = 'bench_result/stat'
    df = read_benchmark_results(file_path)
    save_benchmark_results(df, output_dir)
    calculate_statistics(df)

if __name__ == "__main__":
    main()