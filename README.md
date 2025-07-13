# 🧬 Allergy Data ETL: CSV to PostgreSQL

This project demonstrates a simple **Extract-Transform-Load (ETL)** pipeline built in Python to process allergy patient data and load it into a PostgreSQL database.

---

## 🛠 Technologies Used
- Python
- pandas
- SQLAlchemy
- psycopg2
- PostgreSQL (local)



## ⚙️ How It Works

### 🔹 Extract
Reads the CSV file into a pandas DataFrame.

### 🔹 Transform
- Parses date columns
- Flags active/inactive allergies
- Cleans text data
- Drops duplicates

### 🔹 Load
Pushes the cleaned data to a local PostgreSQL database.

---

## 🚀 How to Run

1. Install dependencies:
```bash
pip install -r requirements.txt
