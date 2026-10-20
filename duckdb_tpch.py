import duckdb

# Creates or opens 'my_database.duckdb' on disk
con = duckdb.connect('/Users/nat/Projects/duckdb/tpch.duckdb')

# Run queries or create tables

results = con.execute("SELECT * FROM tpch.main.partsupp LIMIT 10").fetchall()
print(results)
con.close()


