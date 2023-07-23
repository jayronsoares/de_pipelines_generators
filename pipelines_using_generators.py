import csv
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Set up PostgreSQL with SQLAlchemy
engine = create_engine('postgresql://username:password@localhost/dbname')  # Replace with your PostgreSQL connection string
Base = declarative_base()
Session = sessionmaker(bind=engine)

# Data Model for Database
class ProcessedData(Base):
    __tablename__ = 'processed_data'
    id = Column(Integer, primary_key=True)
    data = Column(String)

# Data Source - Generator Function for Large CSV Parsing with Chunking
def csv_data_source_generator(file_path, chunksize=1000):
    with open(file_path, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip the header row
        chunk = []
        for row in reader:
            chunk.append(row[0])  # Assuming the first column contains the data
            if len(chunk) >= chunksize:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

# Data Processing Steps - Lowercase and Remove Duplicates
def lowercase_transform(data):
    return data.lower()

def remove_duplicates(data_list):
    return list(set(data_list))

# Batch Processing for Database Insertions with Context Manager
@contextmanager
def session_scope():
    session = Session()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

def batch_insert_to_database(data_list):
    with session_scope() as session:
        processed_data_list = remove_duplicates(data_list)
        data_objects = [ProcessedData(data=data) for data in processed_data_list]
        session.bulk_save_objects(data_objects)

# Pipeline Construction
def data_processing_pipeline(file_path):
    data_gen = csv_data_source_generator(file_path)
    for chunk in data_gen:
        chunk = map(lowercase_transform, chunk)
        yield from chunk

# Main Function for Tests
def main():
    input_file = 'input_data.csv'
    batch_size = 1000  # Adjust the batch size according to your requirements

    data_list = []
    for processed_data in data_processing_pipeline(input_file):
        data_list.append(processed_data)
        if len(data_list) >= batch_size:
            batch_insert_to_database(data_list)
            data_list = []

    if data_list:
        batch_insert_to_database(data_list)

    logger.info("Data processing and storage completed.")

if __name__ == "__main__":
    main()
