import sqlite3

# Connect to SQLite database
conn = sqlite3.connect('mpg_eln.db')

# Create a cursor object
cursor = conn.cursor()

# Open and read the file
with open('schema.sql', 'r') as f:
    sql_file = f.read()

# Execute the SQL commands
cursor.executescript(sql_file)

# Commit the transaction
conn.commit()

# Close the connection
conn.close()
