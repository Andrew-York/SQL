# Imports
import sqlite3
import pandas as pd

# Pandas to SQL DB
def pd_to_sqlDB(input_df: pd.DataFrame,
                table_name: str,
                db_name: str = 'default.db') -> None:
  """
  Take a pandas dataframe 'input_df' and upload it to 'table_name' SQLITE table

  Args:
    input_df (pd.DataFrame): Dataframe containing data to upload to SQLITE
    table_name (str): Name of the SQLITE table to upload to
    db_name (str, optional): Name of the SQLITE Database in which the table is created.
                             Defaults to 'default.db'
  """

  # Set up local logging
  import logging
  logging.basicConfig(level=logging.INFO,
                      format='%(asctime)s %(levelname)s: %(message)s',
                      datefmt='%Y-%m-%d %H:%M:%S')

  # Find columns in the dataframe
  cols = input_df.columns
  cols_string = ','.join(cols)
  val_wildcard_string = ','.join(['?'] * len(cols))

  # Connect to a DB file if it exists, else create a new file
  con = sqlite3.connect(db_name)
  cur = con.cursor()
  logging.info(f'SQL DB {db_name} created')

  # Create Table
  sql_string = f"CREATE TABLE {table_name} ({cols_string});"
  cur.execute(sql_string)
  logging.info(f"SQL Table {table_name} created with {len(cols)} columns")

  # Upload the DataFrame
  rows_to_upload = input_df.to_dict(orient='split')['data']
  sql_string = f"INSERT INTO {table_name} ({cols_string}) VALUES ({val_wildcard_string});"
  cur.executemany(sql_string, rows_to_upload)
  logging.info(f"{len(rows_to_upload)} rows uploaded to {table_name}")

  # Commit the changes and close the connection
  con.commit()
  con.close()

# SQL query to PD
def sql_query_to_pd(sql_query_string: str,
                    db_name: str = 'default.db') -> pd.DataFrame:
  """
  Execute an SQL query and return the results as a pandas dataframe.

  Args:
    sql_query_string (str): SQL query string to execute.
    db_name (str, optional): Name of the SQLITE Database to execute the query in.
                             Defaults to 'default.db'.
  Returns:
    pd.DataFrame: Results of the SQL query in a pandas dataframe.
  """

  # Connect to the SQL DB
  con = sqlite3.connect(db_name)

  # Execute the SQL query
  cursor = con.execute(sql_query_string)

  # Fetch the data and column names
  result_data = cursor.fetchall()
  cols = [description[0] for description in cursor.description]

  # Close the connection
  con.close()

  # Return as a dataframe
  return pd.DataFrame(result_data, columns=cols)