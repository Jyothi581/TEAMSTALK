import MySQLdb

print("🔍 Testing connection...")
conn = MySQLdb.connect(
    host="localhost",
    user="root",
    passwd="Jyo7483##",
    db="teamtalk",
    port=3306,
    connect_timeout=5
)
print("✅ Connected successfully!")
