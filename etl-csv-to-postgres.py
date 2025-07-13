import pandas as pd 
from sqlalchemy import create_engine


# ---Extract---
# Read the CSV file into a pandas DataFrame
csv_path = "/Users/edwardjr/Downloads/upwork /Engineering/PostgreSQL_database/allergies.csv"
df = pd.read_csv(csv_path)

print(df.head())
print(df.columns)


# ---Transform---
# date parsing
df["START"] = pd.to_datetime(df["START"], errors='coerce')
df["STOP"] = pd.to_datetime(df["STOP"], errors='coerce')

# filter/normalise dates
df["is_active"] = df["STOP"].isna()
df["duration_days"] = (df["STOP"] - df["START"]).dt.days

# standardise text
df["DESCRIPTION"] = df["DESCRIPTION"].str.strip().str.lower()

# drop duplicates
df.drop_duplicates(inplace=True)

# build the ETL DataFrame
etl_df = df[["PATIENT", "ENCOUNTER", "START", "STOP", "DESCRIPTION", "CODE", "is_active"]]



# ---Load---
# Load the ETL DataFrame into the PostgreSQL database
# PostgreSQL connection details
db_user = 'postgres'
db_pass = 'bankai'
db_host = 'localhost'
db_port = '5432'
db_name = 'etl_project'

engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

# Write the DataFrame to PostgreSQL (replace 'allergies_etl' with your desired table name)
etl_df.to_sql('allergies_etl', engine, if_exists='replace', index=False)

print("Data loaded into PostgreSQL successfully!")







