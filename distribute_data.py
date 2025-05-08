import psycopg2

# Connect to original pagila source (port 5432)
conn_pagila = psycopg2.connect(
    dbname="pagila",
    user="postgres",
    password="123456",  # or your actual password
    host="localhost",
    port="5432"
)

# Connect to shard1 and shard2
conn_shard1 = psycopg2.connect(
    dbname="pagila",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5433"
)

conn_shard2 = psycopg2.connect(
    dbname="pagila",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5434"
)

# Create cursors
cur_source = conn_pagila.cursor()
cur_shard1 = conn_shard1.cursor()
cur_shard2 = conn_shard2.cursor()

# Fetch all payment records
cur_source.execute("SELECT * FROM public.payment;")
all_rows = cur_source.fetchall()

# Get column count for dynamic insert
columns = [desc[0] for desc in cur_source.description]
placeholders = ','.join(['%s'] * len(columns))
insert_query = f"INSERT INTO payment ({', '.join(columns)}) VALUES ({placeholders})"

# Distribute data
for row in all_rows:
    customer_id = row[1]  # assuming 2nd column is customer_id
    if customer_id % 2 == 0:
        cur_shard1.execute(insert_query, row)
    else:
        cur_shard2.execute(insert_query, row)

# Commit changes
conn_shard1.commit()
conn_shard2.commit()

# Print confirmation
print(f"Total rows fetched from source: {len(all_rows)}")
print("Sharding completed.")

# Close all
cur_source.close()
cur_shard1.close()
cur_shard2.close()
conn_pagila.close()
conn_shard1.close()
conn_shard2.close()
