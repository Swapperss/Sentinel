import mysql.connector as m

c = m.connect(host="localhost", port=3306, user="root", password="root")
cur = c.cursor()

# Check migration ledger
cur.execute("SELECT version FROM sentinel_admin.schema_migrations WHERE version=%s", ("006_sentinel_star_schema.sql",))
print("migration_entry=", cur.fetchall())

# Check sentinel tables
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema=%s ORDER BY table_name", ("sentinel",))
print("sentinel_tables=", cur.fetchall())

cur.close()
c.close()