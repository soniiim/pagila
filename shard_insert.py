import psycopg2

# Creds korrigjuar për secilën DB
source_db = psycopg2.connect(
    host="localhost", port=5432, dbname="pagila", user="postgres", password="123456"
)
shard1 = psycopg2.connect(
    host="localhost", port=5433, dbname="pagila", user="postgres", password="postgres"
)
shard2 = psycopg2.connect(
    host="localhost", port=5434, dbname="pagila", user="postgres", password="postgres"
)

# Boshatisni tabelat në shard1/shard2 përpara insertimit
for conn in (shard1, shard2):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE payment;")
    conn.commit()

# Merr të gjitha rreshtat nga tabela "payment" në pagila
with source_db.cursor() as source_cur:
    source_cur.execute(
        "SELECT payment_id, customer_id, staff_id, rental_id, amount, payment_date FROM payment;"
    )
    rows = source_cur.fetchall()

insert_query = """
    INSERT INTO payment (payment_id, customer_id, staff_id, rental_id, amount, payment_date)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (payment_id) DO NOTHING;
"""

# Shpërndaj të dhënat
count = 0
for row in rows:
    target = shard1 if row[1] % 2 == 0 else shard2
    with target.cursor() as cur:
        cur.execute(insert_query, row)
    target.commit()
    count += 1

# Mbyll lidhjet
source_db.close()
shard1.close()
shard2.close()

print(f"✅ Done! Inserted {count} rows into shards.")
