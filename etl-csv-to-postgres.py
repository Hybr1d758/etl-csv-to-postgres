import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import find_dotenv, load_dotenv


dotenv_path = find_dotenv('.env')
load_dotenv(dotenv_path)


def extract(csv_path):
   csv_path = "/Users/edwardjr/Downloads/upwork /Engineering/PostgreSQL_database/allergies.csv"
   return pd.read_csv(csv_path)

def transform_data(df):
   df["START"] = pd.to_datetime(df["START"], errors='coerce')
   df["STOP"] = pd.to_datetime(df["STOP"], errors='coerce')
   df["is_active"] = df["STOP"].isna()
   df["duration_days"] = (df["STOP"] - df["START"]).dt.days
   df["DESCRIPTION"] = df["DESCRIPTION"].str.strip().str.lower()
   df.drop_duplicates(inplace=True)
   return df[["PATIENT", "ENCOUNTER", "START", "STOP", "DESCRIPTION", "CODE", "is_active"]]



def load_data(df, table_name):
    db_user = os.getenv('DB_USER')
    db_pass = os.getenv('DB_PASS')
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME')
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print("Data loaded into PostgreSQL successfully!")

def run():
    csv_path = os.environ.get('CSV_PATH')
    print(f"CSV_PATH: {csv_path}")  # Debug: check if the path is loaded
    df = extract(csv_path)
    etl_df = transform_data(df)
    load_data(etl_df, 'allergies_etl')

if __name__ == "__main__":
    run()







