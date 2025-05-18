# lab1_mongodb_models.py
# Author: <Your Name>
# Group members: <List all members>
# Description: Implements three MongoDB data models (M1, M2, M3) for Person-Company
# relationship. Two modes: 'populate' to create and fill collections, 'queries' to run Q1-Q4.

import os
import time
import argparse
from datetime import datetime
from faker import Faker
from pymongo import MongoClient

# ----------------------------------------------------------------------------
# Data generation
# ----------------------------------------------------------------------------

def generate_companies(fake, count):
    companies = []
    for _ in range(count):
        fd = fake.date_between(start_date='-50y', end_date='today')
        founded = datetime.combine(fd, datetime.min.time())
        companies.append({
            'name': fake.company(),
            'address': fake.address(),
            'founded': founded
        })
    return companies


def generate_persons(fake, count, company_ids):
    persons = []
    for _ in range(count):
        bd = fake.date_of_birth(minimum_age=18, maximum_age=80)
        birth = datetime.combine(bd, datetime.min.time())
        age = int((datetime.now().date() - bd).days // 365)
        cid = fake.random_element(company_ids)
        persons.append({
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'birth_date': birth,
            'age': age,
            'company_id': cid
        })
    return persons

# ----------------------------------------------------------------------------
# Populate pipelines
# ----------------------------------------------------------------------------

def populate_M1(db, companies, persons):
    comp_coll = db.companies_ref
    pers_coll = db.persons_ref
    comp_coll.drop()
    pers_coll.drop()
    comp_ids = comp_coll.insert_many(companies).inserted_ids
    for comp, _id in zip(companies, comp_ids): comp['_id'] = _id
    pers_coll.insert_many(persons)
    dist = list(pers_coll.aggregate([
        {'$group': {'_id': '$company_id', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}}
    ]))
    print("\nM1 populated. Top 5 companies by person count:")
    for d in dist[:5]: print(f"Company {d['_id']}: {d['count']}")
    print(f"Avg persons per company: {len(persons)/len(companies):.2f}\n")


def populate_M2(db, companies, persons):
    pers_coll = db.persons_emb
    pers_coll.drop()
    persons_emb = []
    for p in persons:
        comp = next(c for c in companies if c['_id'] == p['company_id'])
        persons_emb.append({
            'first_name': p['first_name'],
            'last_name': p['last_name'],
            'birth_date': p['birth_date'],
            'age': p['age'],
            'company': {'_id': comp['_id'], 'name': comp['name'], 'address': comp['address']}
        })
    pers_coll.insert_many(persons_emb)
    print(f"M2 populated: persons with embedded company. Count: {len(persons_emb)}")


def populate_M3(db, companies, persons):
    comp_coll = db.companies_emb
    comp_coll.drop()
    comp_docs = []
    for comp in companies:
        emps = [p for p in persons if p['company_id'] == comp['_id']]
        comp_docs.append({
            'name': comp['name'],
            'address': comp['address'],
            'employees': [{'first_name': p['first_name'], 'last_name': p['last_name'], 'birth_date': p['birth_date'], 'age': p['age']} for p in emps]
        })
    comp_coll.insert_many(comp_docs)
    print(f"M3 populated: companies with embedded persons. Count: {len(comp_docs)}")

# ----------------------------------------------------------------------------
# Query pipelines
# ----------------------------------------------------------------------------

def run_queries_M1(db):
    comp_coll = db.companies_ref
    pers_coll = db.persons_ref
    print("\n=== M1 Queries ===")
    # Q1: lookup persons with company names
    start = time.time()
    q1 = list(pers_coll.aggregate([
        {'$lookup': {'from': 'companies_ref', 'localField': 'company_id', 'foreignField': '_id', 'as': 'company'}},
        {'$unwind': '$company'},
        {'$project': {'full_name': {'$concat': ['$first_name',' ', '$last_name']}, 'company_name': '$company.name', '_id': 0}}
    ]))
    t1 = time.time() - start
    print(f"Q1: {len(q1)} docs, time={t1:.4f}s")
    print("Q1 sample:", q1[:3])
    # Q2: count employees per company
    start = time.time()
    q2 = list(comp_coll.aggregate([
        {'$lookup': {'from': 'persons_ref', 'localField': '_id', 'foreignField': 'company_id', 'as': 'emps'}},
        {'$project': {'name':1, 'num_employees': {'$size':'$emps'}, '_id':0}}
    ]))
    t2 = time.time() - start
    print(f"Q2: {len(q2)} docs, time={t2:.4f}s")
    print("Q2 sample:", q2[:3])
    # Q3: update ages for those born before 1988
    start = time.time()
    result3 = pers_coll.update_many({'birth_date': {'$lt': datetime(1988,1,1)}}, {'$set': {'age':30}})
    t3 = time.time() - start
    print(f"Q3: modified {result3.modified_count} docs, time={t3:.4f}s")
    # Q4: append ' Company' to company names
    start = time.time()
    result4 = comp_coll.update_many({}, [{'$set': {'name': {'$concat': ['$name',' Company']}}}])
    t4 = time.time() - start
    print(f"Q4: modified {result4.modified_count} docs, time={t4:.4f}s")


def run_queries_M2(db):
    pers_coll = db.persons_emb
    print("\n=== M2 Queries ===")
    # Q1: full names with company names
    start = time.time()
    q1 = list(pers_coll.aggregate([
        {'$project': {'_id': 0, 'full_name': {'$concat': ['$first_name',' ', '$last_name']}, 'company.name':1}}
    ]))
    t1 = time.time() - start
    print(f"Q1: {len(q1)} docs, time={t1:.4f}s")
    print("Q1 sample:", q1[:3])
    # Q2: count of employees per company
    start = time.time()
    q2 = list(pers_coll.aggregate([
        {'$group': {'_id': '$company._id', 'name': {'$first': '$company.name'}, 'num_employees': {'$sum': 1}}},
        {'$project': {'_id':0,'name':1,'num_employees':1}}
    ]))
    t2 = time.time() - start
    print(f"Q2: {len(q2)} companies, time={t2:.4f}s")
    print("Q2 sample:", q2[:3])
    # Q3: update ages for those born before 1988
    start = time.time()
    result3 = pers_coll.update_many({'birth_date': {'$lt': datetime(1988,1,1)}}, {'$set': {'age':30}})
    t3 = time.time() - start
    print(f"Q3: modified {result3.modified_count} docs, time={t3:.4f}s")
    # Q4: append ' Company' to company names
    start = time.time()
    result4 = pers_coll.update_many({}, {'$set': {'company.name': {'$concat': ['$company.name',' Company']}}})
    t4 = time.time() - start
    print(f"Q4: modified {result4.modified_count} docs, time={t4:.4f}s")


def run_queries_M3(db):
    comp_coll = db.companies_emb
    print("\n=== M3 Queries ===")
    # Q1: unwind employees to get full names
    start = time.time()
    q1 = list(comp_coll.aggregate([
        {'$unwind': '$employees'},
        {'$project': {'full_name': {'$concat': ['$employees.first_name',' ', '$employees.last_name']}, 'company_name': '$name', '_id':0}}
    ]))
    t1 = time.time() - start
    print(f"Q1: {len(q1)} docs, time={t1:.4f}s")
    print("Q1 sample:", q1[:3])
    # Q2: count employees per company
    start = time.time()
    q2 = list(comp_coll.aggregate([
        {'$project': {'name':1,'num_employees': {'$size':'$employees'}, '_id':0}}
    ]))
    t2 = time.time() - start
    print(f"Q2: {len(q2)} companies, time={t2:.4f}s")
    print("Q2 sample:", q2[:3])
    # Q3: update embedded ages for those born before 1988
    start = time.time()
    result3 = comp_coll.update_many(
        {'employees.birth_date': {'$lt': datetime(1988,1,1)}},
        {'$set': {'employees.$[e].age':30}}, array_filters=[{'e.birth_date': {'$lt': datetime(1988,1,1)}}]
    )
    t3 = time.time() - start
    print(f"Q3: modified {result3.modified_count} docs, time={t3:.4f}s")
    # Q4: append ' Company' to company names
    start = time.time()
    result4 = comp_coll.update_many({}, [{'$set': {'name': {'$concat': ['$name',' Company']}}}])
    t4 = time.time() - start
    print(f"Q4: modified {result4.modified_count} docs, time={t4:.4f}s")

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Lab1 MongoDB: populate or run queries')
    sub = parser.add_subparsers(dest='action', required=True)
    pop = sub.add_parser('populate', help='Create and populate collections')
    pop.add_argument('--uri', type=str, default=os.getenv('MONGODB_URI','mongodb://localhost:27017'))
    pop.add_argument('--db', type=str, default='lab1_db')
    pop.add_argument('--n_companies', type=int, default=5)
    pop.add_argument('--n_persons', type=int, default=50000)
    qry = sub.add_parser('queries', help='Run queries on existing data')
    qry.add_argument('--uri', type=str, default=os.getenv('MONGODB_URI','mongodb://localhost:27017'))
    qry.add_argument('--db', type=str, default='lab1_db')
    args = parser.parse_args()

    client = MongoClient(args.uri)
    db = client[args.db]

    if args.action == 'populate':
        fake = Faker()
        companies = generate_companies(fake, args.n_companies)
        temp_coll = db['__temp_comp']
        temp_coll.drop()
        comp_ids = temp_coll.insert_many(companies).inserted_ids
        temp_coll.drop()
        for comp, _id in zip(companies, comp_ids): comp['_id'] = _id
        persons = generate_persons(fake, args.n_persons, comp_ids)
        print(f"Generated {len(companies)} companies and {len(persons)} persons.")
        populate_M1(db, companies, persons)
        populate_M2(db, companies, persons)
        populate_M3(db, companies, persons)
    else:
        run_queries_M1(db)
        run_queries_M2(db)
        run_queries_M3(db)
