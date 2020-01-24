# Ingest titanic data
import pandas as pd
import psycopg2

# Ingest and Wrangle data for export to SQL

# ingest CSV data
titanic_url = 'https://raw.githubusercontent.com/gptix/DS-Unit-3-Sprint-2-SQL-and-Databases/master/module2-sql-for-analysis/titanic.csv'
titanic_df = pd.read_csv(titanic_url)

# Remove characters that confuse SQL
titanic_df['Name'] = titanic_df['Name'].str.replace(r'[\“\‘\',]', "")

# Replace slashes with underscores.
# titanic_df.columns = list(titanic_df.columns)
titanic_df.columns = ['Survived', 'Pclass', 'Name', 'Sex', 'Age',
                      'Siblings_Spouses_Aboard', 'Parents_Children_Aboard',
                      'Fare']


# Prepare connection to Elephant
dbname = 'sjnmahbj'
user = 'sjnmahbj'
password = 'not here. Call!'
port = '5432'
host = 'rajje.db.elephantsql.com'  # port should be included by defult

# Get a connection
pg_conn = psycopg2.connect(dbname=dbname, user=user,
                          password=password, host=host)

# Create table.  This only needs to be uncommented if no table
# exists on Elephant.
# create_titanic_table_SQL = """
# CREATE TABLE titanic (
#   id                      SERIAL PRIMARY KEY,
#   Survived                INT,
#   Pclass                  INT,
#   Name                    VARCHAR(200),
#   Sex                     VARCHAR(6),
#   Age                     INT,
#   Siblings_Spouses_Aboard INT,
#   Parents_Children_Aboard INT,
#   Fare                    REAL
# );"""
# # print(create_titanic_table_SQL)

# # dbname = 'sjnmahbj'
# # user = 'sjnmahbj'
# # password = 'not real'
# # port = '5432'
# # host = 'rajje.db.elephantsql.com' # port should be included by defult
# pg_curs = pg_conn.cursor()
# pg_curs.execute(create_titanic_table_SQL)
# pg_conn.commit()


# Create column list in format usable by SQL (no quotes, parens
# instead of brackets
colnames_SQL = str(list(titanic_df.columns))
colnames_SQL = colnames_SQL.replace("[", "(")
colnames_SQL = colnames_SQL.replace("]", ")")
colnames_SQL = colnames_SQL.replace('\'', '')
# colnames_SQL

# These functions convert a Pandas dataframe to an SQL statement,
# and executre that using the pg_connection.


def row_to_str(row):
    """Convert a df row to a string for insert into SQL database."""
    return str(list(row)).replace("[", "(").replace("]", ")")


def make_SQL_row_insert_query(row):
    """Make an insert statement for one row at a time."""
    return """INSERT INTO titanic """ + colnames_SQL + """
VALUES """ + row_to_str(row) + ";"


# Renew connection
pg_conn = psycopg2.connect(dbname=dbname, user=user,
                           password=password, host=host)


def insert_row(row):
    """Given a titanic_df row from values, build and execute an insert."""
    pg_curs = pg_conn.cursor()
    pg_curs.execute(make_SQL_row_insert_query(row))
    pg_conn.commit()


# POPULATE TABLE IN CLOUD
# COMPLETE -RUNNING AGAIN WILL ADD MORE ROWS
# pg_conn = psycopg2.connect(dbname=dbname, user=user,
#                           password=password, host=host)
# for value_row in titanic_df.values:
#     insert_row(value_row)


# Answers using the database.

# How many passengers were in each class?
SQL1 = """
SELECT Pclass, COUNT (Pclass) FROM titanic
GROUP BY Pclass
ORDER BY Pclass ASC
"""
pg_curs = pg_conn.cursor()
pg_curs.execute(SQL1)
pg_conn.commit()
results = pg_curs.fetchall()

print("Passenger count per passenger class")
for r in results:
    print(f"Passenger class: {r[0]} - Passenger count: {r[1]}")
print()


# How many passengers survived/died within each class?
pg_conn = psycopg2.connect(dbname=dbname, user=user,
                           password=password, host=host)
SQL2 = """SELECT Pclass, COUNT (survived) FROM titanic
WHERE survived = 1
GROUP BY Pclass
ORDER BY Pclass ASC
"""
pg_curs = pg_conn.cursor()
pg_curs.execute(SQL2)
pg_conn.commit()
results = pg_curs.fetchall()
results

print("Survivor count per passenger class")
for r in results:
    print(f"Passenger class: {r[0]} - Survivor count: {r[1]}")


# What was the average age of survivors vs nonsurvivors?
SQL6 = """
SELECT Survived, AVG (age) FROM titanic
GROUP BY Survived
ORDER BY Survived ASC
"""
pg_curs = pg_conn.cursor()
pg_curs.execute(SQL6)
pg_conn.commit()
results = pg_curs.fetchall()
results

print("Average age by Survival")
for r in results:
    if (r[0] == 0):
        survived = "No"
    else:
        survived = "Yes"
    print(f"Survived?: {survived} - Average age: {round(r[1],1)}")
print()

# What was the average age of each passenger class?
SQL3 = """
SELECT Pclass, AVG (age) FROM titanic
GROUP BY Pclass
ORDER BY Pclass ASC
"""
pg_curs = pg_conn.cursor()
pg_curs.execute(SQL3)
pg_conn.commit()
results = pg_curs.fetchall()
results

print("Average age by passenger class")
for r in results:
    print(f"Passenger class: {r[0]} - Average age: {round(r[1],1)}")
print()

# What was the average fare by passenger class?
SQL4 = """
SELECT Pclass, AVG (fare) FROM titanic
GROUP BY Pclass
ORDER BY Pclass ASC
"""
pg_curs = pg_conn.cursor()
pg_curs.execute(SQL4)
pg_conn.commit()
results = pg_curs.fetchall()
# results

print("Average age by passenger class")
for r in results:
    print(f"Passenger class: {r[0]} - Average fare: {round(r[1],2)}")
print()

# How many siblings/spouses aboard on average, by passenger class?
SQL7 = """
SELECT Pclass, AVG (siblings_spouses_aboard) FROM titanic
GROUP BY Pclass
ORDER BY Pclass ASC
"""
pg_curs = pg_conn.cursor()
pg_curs.execute(SQL7)
pg_conn.commit()
results = pg_curs.fetchall()
# results

print("Average count of siblings/spouses aboard by passenger class")
for r in results:
    print(f"Passenger class: {r[0]} - Average count of siblings/spouses aboard: {round(r[1],2)}")
print()

# How many siblings/spouses aboard on average, by survival?
SQL8 = """
SELECT Survived, AVG (siblings_spouses_aboard) FROM titanic
GROUP BY Survived
ORDER BY Survived ASC
"""
pg_curs = pg_conn.cursor()
pg_curs.execute(SQL8)
pg_conn.commit()
results = pg_curs.fetchall()
# results

print("Average count of siblings/spouses aboard by survival")
for r in results:
    if (r[0] == 0):
        survived = "No"
    else:
        survived = "Yes"
    print(f"Survived?: {survived} - Average count of siblings/spouses aboard: {round(r[1],2)}")
print()

# How many parents/children aboard on average, by passenger class?
SQL9 = """
SELECT Pclass, AVG (parents_children_aboard) FROM titanic
GROUP BY Pclass
ORDER BY Pclass ASC
"""
pg_curs = pg_conn.cursor()
pg_curs.execute(SQL9)
pg_conn.commit()
results = pg_curs.fetchall()
# results

print("Average count of  parents/children aboard by passenger class")
for r in results:
    print(f"Passenger class: {r[0]} - Average count of parents/children aboard: {round(r[1],2)}")
print()

# How many parents/children aboard on average, by survival?
SQL10 = """
SELECT Survived, AVG (parents_children_aboard) FROM titanic
GROUP BY Survived
ORDER BY Survived ASC
"""
pg_curs = pg_conn.cursor()
pg_curs.execute(SQL10)
pg_conn.commit()
results = pg_curs.fetchall()
# results

print("Average count of parents and children aboard, by survival")
for r in results:
    if (r[0] == 0):
        survived = "No"
    else:
        survived = "Yes"
    print(f"Survived?: {survived} - Average count of parents and children aboard: {round(r[1],2)}")
print()

# Renew connection
pg_conn = psycopg2.connect(dbname=dbname, user=user,
                           password=password, host=host)

# Count records
SQL12 = """
SELECT COUNT (*) FROM titanic
"""
pg_curs = pg_conn.cursor()
pg_curs.execute(SQL12)
pg_conn.commit()
results = pg_curs.fetchall()
record_count = results[0][0]

# Do any passengers have the same name?
SQL13 = """
SELECT Name FROM titanic
GROUP BY Name
HAVING COUNT(*) > 1
"""
pg_curs = pg_conn.cursor()
pg_curs.execute(SQL13)
pg_conn.commit()
results = pg_curs.fetchall()
if (len(results) == 0):
    print("No passenger had the same name as another.")
else:
    print("At least two passengers had the same name as another.")
print()
