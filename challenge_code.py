import psycopg2
import pandas as pd
from datetime import timedelta

# Connect to the AWS RDS Postgres Database
connection = psycopg2.connect(
    host = 'keet-health-challenge.cx6uy7iy3oyf.us-east-2.rds.amazonaws.com',
    port = 5432,
    user = 'keet_health',
    password = 'keet_PW_3853',
    database='user_data'
    )
cursor=connection.cursor()

# Retrieve the data from the users table
users_query = """
SELECT *
FROM users
"""
users = pd.read_sql(users_query, con=connection)
print('users data from Postgres:')
print(users.info())
print(users)

# Calculate the number of users per day
users['visit_date'] = pd.to_datetime(users['visit_date'])
daily_counts = users.groupby('visit_date')['id'].count().reset_index().rename(columns={'id': 'count'})
print('daily user counts:')
print(daily_counts)

# Estimate the number of users 1 day in the future
print('-' * 50)
max_date = daily_counts['visit_date'].max()
print('most recent date:', max_date)
future_date = max_date + timedelta(days=1)
print('1 day in the future:', future_date)
future_count = int(round(daily_counts['count'].mean()))
print('expected count 1 day in the future:', future_count)
daily_counts = daily_counts.append([{'visit_date': future_date, 'count': future_count}])

# Get the year, month, and date, and set the observed value
daily_counts['year'] = daily_counts['visit_date'].dt.year
daily_counts['month'] = daily_counts['visit_date'].dt.month
daily_counts['day'] = daily_counts['visit_date'].dt.day
print('-' * 50)

print('output data (excluding observed):')
print(daily_counts[['year', 'month', 'day', 'count']])

# Construct the query to insert the data into the daily_user_counts table
print('-' * 50)
insert_query = """INSERT INTO daily_user_counts
VALUES """
text_rows = []
for r in daily_counts.iterrows():
    row_data = [str(r[1]['year']), str(r[1]['month']), str(r[1]['day']), 'NULL', str(r[1]['count'])]
    row_text = f"({', '.join(row_data)})"
    text_rows.append(row_text)
text_rows = ",\n".join(text_rows)
insert_query += text_rows
print('query to insert the data:')
print(insert_query)

# Insert the data into the daily_user_counts table
cursor.execute(insert_query)
connection.commit()
print('Data successfully inserted into the daily_user_counts table')
