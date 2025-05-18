# clean_db.py
# Author: <Your Name>
# Description: Standalone script to drop a specified MongoDB database.

import os
import argparse
from pymongo import MongoClient


def clean_database(uri: str, db_name: str):
    """Connects to MongoDB and drops the specified database."""
    client = MongoClient(uri)
    client.drop_database(db_name)
    print(f"Dropped database '{db_name}' at '{uri}'.")


def main():
    parser = argparse.ArgumentParser(description='Drop a MongoDB database')
    parser.add_argument(
        '--uri',
        type=str,
        default=os.getenv('MONGODB_URI', 'mongodb://admin:safepwd1@89.110.124.197:27017/admin?authSource=admin'),
        help='MongoDB connection URI'
    )
    parser.add_argument(
        '--db',
        type=str,
        default='lab1_db',
        help='Name of the database to drop'
    )
    args = parser.parse_args()

    clean_database(args.uri, args.db)


if __name__ == '__main__':
    main()
