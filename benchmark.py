import psycopg2
import time
from concurrent.futures import ThreadPoolExecutor

# Kredencialet e sakta:
connections = {
    'Monolithic DB': {
        'host': 'localhost', 'port': 5432,
        'dbname': 'pagila', 'user': 'postgres', 'password': '123456'
    },
    'Shard 1': {
        'host': 'localhost', 'port': 5433,
        'dbname': 'pagila', 'user': 'postgres', 'password': 'postgres'
    },
    'Shard 2': {
        'host': 'localhost', 'port': 5434,
        'dbname': 'pagila', 'user': 'postgres', 'password': 'postgres'
    },
}

query = "SELECT staff_id, SUM(amount) FROM payment GROUP BY staff_id;"

def run_benchmark(name, conn_info):
    try:
        conn = psycopg2.connect(**conn_info)
        cur = conn.cursor()
        start = time.time()
        cur.execute(query)
        cur.fetchall()
        elapsed = time.time() - start
        cur.close()
        conn.close()
        print(f"{name}: {elapsed:.6f} seconds")
        return elapsed
    except Exception as e:
        print(f"{name}: Error → {e}")
        return None

if __name__ == "__main__":
    print("🔍 Benchmarking query across databases:")
    # Para shardimit
    time_before = run_benchmark('Monolithic DB', connections['Monolithic DB'])
    # Pas shardimit në paralel
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(run_benchmark, 'Shard 1', connections['Shard 1']),
            executor.submit(run_benchmark, 'Shard 2', connections['Shard 2']),
        ]
        results = [f.result() for f in futures]
    total_after = sum(r for r in results if r is not None)
    print(f"Total after sharding: {total_after:.6f} seconds")
