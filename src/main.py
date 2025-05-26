import os
import time
import argparse
import gc
from datetime import datetime
from typing import List, Dict, Any, Optional

from bson import ObjectId
from faker import Faker
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

# ----------------------------------------------------------------------------
# Optimized batch processing functions
# ----------------------------------------------------------------------------


def generate_and_populate_companies_batched(
    db: Database,
    fake: Faker,
    total_companies: int,
    batch_size: int = 50_000,
) -> List[ObjectId]:
    """
    Generate total_companies fake companies in batches and insert them into
    the companies_ref collection.

    Indexes for name, founded date and full-text search are created up front
    to keep subsequent operations fast.

    :param db: Target MongoDB database.
    :param fake: Faker instance for synthetic data.
    :param total_companies: Total number of company documents to create.
    :param batch_size: Chunk size for bulk inserts.
    :return: List of inserted ObjectIds in creation order.
    """
    comp_coll: Collection = db.companies_ref
    comp_coll.drop()

    # Create indexes once; they are automatically retained across batches.
    comp_coll.create_index([("name", 1)])
    comp_coll.create_index([("founded", 1)])
    comp_coll.create_index([("name", "text")])

    print(f"Generating and inserting {total_companies} companies "
          f"in batches of {batch_size}...")

    all_company_ids: List[ObjectId] = []

    for i in range(0, total_companies, batch_size):
        current_batch_size = min(batch_size, total_companies - i)

        company_batch: List[Dict[str, Any]] = []
        for _ in range(current_batch_size):
            founded_date = fake.date_between(start_date="-50y", end_date="today")
            company_batch.append(
                {
                    "name": fake.company(),
                    "address": fake.address(),
                    "founded": datetime.combine(founded_date, datetime.min.time()),
                }
            )

        # Ordered=False lets Mongo skip duplicates instead of aborting the batch.
        result = comp_coll.insert_many(company_batch, ordered=False)
        all_company_ids.extend(result.inserted_ids)

        print(
            f"Inserted company batch {i // batch_size + 1}/"
            f"{(total_companies - 1) // batch_size + 1}"
        )

        del company_batch
        gc.collect()

    return all_company_ids


def generate_and_populate_persons_batched(
    db: Database,
    fake: Faker,
    total_persons: int,
    company_ids: List[ObjectId],
    batch_size: int = 100_000,
) -> None:
    """
    Generate `total_persons` fake people and insert them into **persons_ref**
    in batches, each linked to an existing company via *company_id*.

    :param db: Target MongoDB database.
    :param fake: Faker instance for synthetic data.
    :param total_persons: Number of person documents to create.
    :param company_ids: List of valid ObjectIds from **companies_ref**.
    :param batch_size: Chunk size for bulk inserts.
    """
    pers_coll: Collection = db.persons_ref
    pers_coll.drop()

    # Indexes chosen to support common query patterns.
    pers_coll.create_index([("company_id", 1)])
    pers_coll.create_index([("birth_date", 1)])
    pers_coll.create_index([("age", 1)])
    pers_coll.create_index([("last_name", 1)])
    pers_coll.create_index([("first_name", 1), ("last_name", 1)])

    print(f"Generating and inserting {total_persons} persons "
          f"in batches of {batch_size}...")

    num_companies = len(company_ids)

    for i in range(0, total_persons, batch_size):
        current_batch_size = min(batch_size, total_persons - i)

        person_batch: List[Dict[str, Any]] = []
        for _ in range(current_batch_size):
            bd = fake.date_of_birth(minimum_age=18, maximum_age=80)
            age = int((datetime.now().date() - bd).days // 365.25)
            person_batch.append(
                {
                    "first_name": fake.first_name(),
                    "last_name": fake.last_name(),
                    "birth_date": datetime.combine(bd, datetime.min.time()),
                    "age": age,
                    "company_id": company_ids[fake.random_int(0, num_companies - 1)],
                }
            )

        pers_coll.insert_many(person_batch, ordered=False)

        print(
            f"Inserted person batch {i // batch_size + 1}/"
            f"{(total_persons - 1) // batch_size + 1}"
        )

        del person_batch
        gc.collect()


def populate_M2_optimized(
    db: Database,
    batch_size: int = 5_000,
) -> None:
    """
    Build the persons_emb collection  by embedding each person's
    company sub-document, pulling source data directly from the DB in
    streaming batches.

    :param db: Target MongoDB database.
    :param companies_info: Unused in the optimized path (kept for signature
                           parity with the non-optimized variant).
    :param persons_info: Unused in the optimized path.
    :param batch_size: Number of persons processed per pipeline iteration.
    """
    pers_coll: Collection = db.persons_ref
    pers_emb_coll: Collection = db.persons_emb
    comp_coll: Collection = db.companies_ref
    pers_emb_coll.drop()

    pers_emb_coll.create_index([("company._id", 1)])
    pers_emb_coll.create_index([("birth_date", 1)])
    pers_emb_coll.create_index([("age", 1)])
    pers_emb_coll.create_index([("first_name", 1), ("last_name", 1)])
    pers_emb_coll.create_index([("company.name", 1)])

    print(f"Creating M2: persons with embedded company data "
          f"(batch size: {batch_size})...")

    total_persons = pers_coll.count_documents({})
    processed = 0

    cursor = pers_coll.find().batch_size(batch_size)
    batch_persons: List[Dict[str, Any]] = []

    for person in cursor:
        batch_persons.append(person)

        if len(batch_persons) >= batch_size:
            _insert_person_batches(batch_persons, comp_coll, pers_emb_coll)
            processed += len(batch_persons)
            print(f"Inserted M2 batch: {processed}/{total_persons} persons processed")
            batch_persons = []

    if batch_persons:
        _insert_person_batches(batch_persons, comp_coll, pers_emb_coll)
        processed += len(batch_persons)
        print(f"Inserted M2 final batch: {processed}/{total_persons} persons processed")

    print(f"M2 populated: {total_persons} persons with embedded company data")


def _insert_person_batches(
    persons: List[Dict[str, Any]],
    comp_coll: Collection,
    pers_emb_coll: Collection,
) -> None:
    """
    Helper to transform and bulk-insert a list of person docs into persons_emb

    :param persons: Chunk of person documents.
    :param comp_coll: **companies_ref** collection handle.
    :param pers_emb_coll: **persons_emb** collection handle.
    """
    company_ids = list({p["company_id"] for p in persons})
    company_dict = {
        c["_id"]: c
        for c in comp_coll.find({"_id": {"$in": company_ids}})
    }

    persons_emb = [
        {
            "first_name": p["first_name"],
            "last_name": p["last_name"],
            "birth_date": p["birth_date"],
            "age": p["age"],
            "company": {
                "_id": company_dict[p["company_id"]]["_id"],
                "name": company_dict[p["company_id"]]["name"],
                "address": company_dict[p["company_id"]]["address"],
            },
        }
        for p in persons
    ]

    pers_emb_coll.insert_many(persons_emb, ordered=False)


def populate_M3_optimized(
    db: Database,
    batch_size: int = 500,
) -> None:
    """
    Build the **companies_emb** collection (Model 3) by aggregating employees
    per company and embedding them.

    The aggregation is fully server-side, then results are up-serted in chunks.

    :param db: Target MongoDB database.
    :param companies_info: Unused in the optimized path.
    :param persons_info: Unused in the optimized path.
    :param batch_size: Number of companies inserted per batch.
    """
    comp_coll: Collection = db.companies_emb
    pers_coll: Collection = db.persons_ref
    comp_coll.drop()

    comp_coll.create_index([("name", 1)])
    comp_coll.create_index([("founded", 1)])
    comp_coll.create_index([("employees.last_name", 1)])
    comp_coll.create_index([("employees.birth_date", 1)])
    comp_coll.create_index([("name", "text")])

    print("Creating M3: companies with embedded employees using aggregation pipeline...")

    # Group employees by company_id.
    pipeline = [
        {
            "$group": {
                "_id": "$company_id",
                "employees": {
                    "$push": {
                        "first_name": "$first_name",
                        "last_name": "$last_name",
                        "birth_date": "$birth_date",
                        "age": "$age",
                    }
                },
            }
        }
    ]

    print("Aggregating employees by company...")
    company_employees: Dict[ObjectId, List[Dict[str, Any]]] = {
        res["_id"]: res["employees"]
        for res in pers_coll.aggregate(pipeline, allowDiskUse=True)
    }

    print(f"Creating M3 documents in batches of {batch_size}...")

    companies_cursor = db.companies_ref.find()
    companies_batch: List[Dict[str, Any]] = []
    batch_count = 0

    for comp in companies_cursor:
        companies_batch.append(comp)

        if len(companies_batch) >= batch_size:
            _insert_company_batches(companies_batch, company_employees, comp_coll)
            batch_count += 1
            print(f"Inserted M3 batch {batch_count}")
            companies_batch = []

    if companies_batch:
        _insert_company_batches(companies_batch, company_employees, comp_coll)
        batch_count += 1
        print(f"Inserted M3 final batch {batch_count}")

    print(
        f"M3 populated: approximately {batch_count * batch_size} companies "
        "with embedded employees"
    )


def _insert_company_batches(
    companies: List[Dict[str, Any]],
    company_employees: Dict[ObjectId, List[Dict[str, Any]]],
    comp_coll: Collection,
) -> None:
    """
    Helper to embed employee lists into company docs and bulk-insert them into companies_emb

    :param companies: Chunk of company documents.
    :param company_employees: Mapping of company_id to its employee list.
    :param comp_coll: companies_emb collection handle.
    """
    comp_docs = [
        {
            "name": c["name"],
            "address": c["address"],
            "founded": c["founded"],
            "employees": company_employees.get(c["_id"], []),
        }
        for c in companies
    ]
    comp_coll.insert_many(comp_docs, ordered=False)

# ----------------------------------------------------------------------------
# Data generation
# ----------------------------------------------------------------------------

def generate_companies(fake: Faker, count: int) -> List[Dict[str, Any]]:
    """
    Return a list of `count` synthetic company dictionaries.

    :param fake: Faker instance.
    :param count: Number of companies to generate.
    """
    companies: List[Dict[str, Any]] = []
    for _ in range(count):
        fd = fake.date_between(start_date="-50y", end_date="today")
        companies.append(
            {
                "name": fake.company(),
                "address": fake.address(),
                "founded": datetime.combine(fd, datetime.min.time()),
            }
        )
    return companies

def generate_persons(
    fake: Faker, count: int, company_ids: List[ObjectId]
) -> List[Dict[str, Any]]:
    """
    Return a list of `count` synthetic person dictionaries.

    :param fake: Faker instance.
    :param count: Number of persons to generate.
    :param company_ids: Valid company ObjectIds to choose from.
    """
    persons: List[Dict[str, Any]] = []
    for _ in range(count):
        bd = fake.date_of_birth(minimum_age=18, maximum_age=80)
        age = int((datetime.now().date() - bd).days // 365)
        persons.append(
            {
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "birth_date": datetime.combine(bd, datetime.min.time()),
                "age": age,
                "company_id": fake.random_element(company_ids),
            }
        )
    return persons

# ----------------------------------------------------------------------------
# Populate pipelines 
# ----------------------------------------------------------------------------

def populate_M1(
    db: Database,
    companies: List[Dict[str, Any]],
    persons: List[Dict[str, Any]],
    batch_size: int = 10_000,
) -> None:
    """
    Store *companies* and *persons* in reference collections (**companies_ref**
    and **persons_ref**) using bulk inserts.

    :param db: Target MongoDB database.
    :param companies: Pre-generated company documents (with *_id* attached).
    :param persons: Pre-generated person documents.
    :param batch_size: Chunk size for inserts.
    """
    comp_coll: Collection = db.companies_ref
    pers_coll: Collection = db.persons_ref
    comp_coll.drop()
    pers_coll.drop()

    print(f"Inserting {len(companies)} companies in batches of {batch_size}...")
    comp_ids: List[ObjectId] = []

    for i in range(0, len(companies), batch_size):
        batch = companies[i : i + batch_size]
        batch_ids = comp_coll.insert_many(batch).inserted_ids
        comp_ids.extend(batch_ids)
        print(
            f"Inserted companies batch {i // batch_size + 1}/"
            f"{(len(companies) - 1) // batch_size + 1}"
        )

    # Sync auto-generated ids back into local objects.
    for comp, _id in zip(companies, comp_ids):
        comp["_id"] = _id

    print(f"Inserting {len(persons)} persons in batches of {batch_size}...")
    for i in range(0, len(persons), batch_size):
        batch = persons[i : i + batch_size]
        pers_coll.insert_many(batch)
        print(
            f"Inserted persons batch {i // batch_size + 1}/"
            f"{(len(persons) - 1) // batch_size + 1}"
        )

    # Display a quick distribution summary
    dist = list(
        pers_coll.aggregate(
            [
                {"$group": {"_id": "$company_id", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
            ]
        )
    )
    print("\nM1 populated. Top 5 companies by person count:")
    for d in dist[:5]:
        print(f"Company {d['_id']}: {d['count']}")
    print(f"Avg persons per company: {len(persons)/len(companies):.2f}\n")


def populate_M2(
    db: Database,
    companies: List[Dict[str, Any]],
    persons: List[Dict[str, Any]],
    batch_size: int = 10_000,
) -> None:
    """
    Build Model 2 (embedded company inside person) from in-memory persons and companies lists.

    :param db: Target MongoDB database.
    :param companies: Company reference list with *_id*.
    :param persons: Person reference list.
    :param batch_size: Chunk size for inserts.
    """
    pers_coll: Collection = db.persons_emb
    pers_coll.drop()

    print("Creating M2: persons with embedded company data...")
    print(f"Processing {len(persons)} persons in batches of {batch_size}...")

    for i in range(0, len(persons), batch_size):
        batch_persons = persons[i : i + batch_size]
        persons_emb = [
            {
                "first_name": p["first_name"],
                "last_name": p["last_name"],
                "birth_date": p["birth_date"],
                "age": p["age"],
                "company": next(
                    {
                        "_id": c["_id"],
                        "name": c["name"],
                        "address": c["address"],
                    }
                    for c in companies
                    if c["_id"] == p["company_id"]
                ),
            }
            for p in batch_persons
        ]

        pers_coll.insert_many(persons_emb)
        print(
            f"Inserted M2 batch {i // batch_size + 1}/"
            f"{(len(persons) - 1) // batch_size + 1}"
        )

    print(f"M2 populated: persons with embedded company. Count: {len(persons)}")


def populate_M3(
    db: Database,
    companies: List[Dict[str, Any]],
    persons: List[Dict[str, Any]],
    batch_size: int = 1_000,
) -> None:
    """
    Build M3 from in-memory data

    :param db: Target MongoDB database.
    :param companies: Company reference list with *_id*.
    :param persons: Person reference list.
    :param batch_size: Companies processed per insert batch.
    """
    comp_coll: Collection = db.companies_emb
    comp_coll.drop()

    print("Creating M3: companies with embedded employees...")
    print(f"Processing {len(companies)} companies in batches of {batch_size}...")

    for i in range(0, len(companies), batch_size):
        batch_companies = companies[i : i + batch_size]
        comp_docs = [
            {
                "name": comp["name"],
                "address": comp["address"],
                "founded": comp["founded"],
                "employees": [
                    {
                        "first_name": p["first_name"],
                        "last_name": p["last_name"],
                        "birth_date": p["birth_date"],
                        "age": p["age"],
                    }
                    for p in persons
                    if p["company_id"] == comp["_id"]
                ],
            }
            for comp in batch_companies
        ]

        comp_coll.insert_many(comp_docs)
        print(
            f"Inserted M3 batch {i // batch_size + 1}/"
            f"{(len(companies) - 1) // batch_size + 1}"
        )

    print(f"M3 populated: companies with embedded persons. Count: {len(companies)}")


# ----------------------------------------------------------------------------
# Query pipelines
# ----------------------------------------------------------------------------

def run_queries_M1(db: Database) -> None:
    """
    Execute demonstration queries against Model 1 (reference collections) and
    print timing / sample results.
    """
    comp_coll = db.companies_ref
    pers_coll = db.persons_ref
    print("\n=== M1 Queries ===")

    # Q1: lookup persons with company names
    start = time.time()
    q1 = list(
        pers_coll.aggregate(
            [
                {
                    "$lookup": {
                        "from": "companies_ref",
                        "localField": "company_id",
                        "foreignField": "_id",
                        "as": "company",
                    }
                },
                {"$unwind": "$company"},
                {
                    "$project": {
                        "full_name": {"$concat": ["$first_name", " ", "$last_name"]},
                        "company_name": "$company.name",
                        "_id": 0,
                    }
                },
            ]
        )
    )
    t1 = time.time() - start
    print(f"Q1: {len(q1)} docs, time={t1:.4f}s")
    print("Q1 sample:", q1[:3])

    # Q2: count employees per company (including companies with 0 employees)
    start = time.time()
    q2 = list(
        comp_coll.aggregate(
            [
                {
                    "$lookup": {
                        "from": "persons_ref",
                        "localField": "_id",
                        "foreignField": "company_id",
                        "as": "employees",
                    }
                },
                {
                    "$project": {
                        "name": 1,
                        "num_employees": {"$size": "$employees"},
                        "_id": 0,
                    }
                },
                {"$sort": {"num_employees": -1}},
            ]
        )
    )
    t2 = time.time() - start
    print(f"Q2: {len(q2)} companies, time={t2:.4f}s")
    print("Q2 sample:", q2[:3])

    # Q3: update ages for those born before 1988-01-01
    start = time.time()
    result3 = pers_coll.update_many(
        {"birth_date": {"$lt": datetime(1988, 1, 1)}}, {"$set": {"age": 30}}
    )
    t3 = time.time() - start
    print(f"Q3: modified {result3.modified_count} docs, time={t3:.4f}s")

    # Q4: append ' Company' to company names (only if not already present)
    start = time.time()
    result4 = comp_coll.update_many(
        {"name": {"$not": {"$regex": " Company$"}}},
        [{"$set": {"name": {"$concat": ["$name", " Company"]}}}],
    )
    t4 = time.time() - start
    print(f"Q4: modified {result4.modified_count} docs, time={t4:.4f}s")


def run_queries_M2(db: Database) -> None:
    """
    Execute demonstration queries against Model 2 (person-embedded company).
    """
    pers_coll = db.persons_emb
    print("\n=== M2 Queries ===")

    # Q1: full names with company names
    start = time.time()
    q1 = list(
        pers_coll.aggregate(
            [
                {
                    "$project": {
                        "_id": 0,
                        "full_name": {"$concat": ["$first_name", " ", "$last_name"]},
                        "company.name": 1,
                    }
                }
            ]
        )
    )
    t1 = time.time() - start
    print(f"Q1: {len(q1)} docs, time={t1:.4f}s")
    print("Q1 sample:", q1[:3])

    # Q2: count of employees per company (including companies with 0 employees)
    start = time.time()
    comp_coll = db.companies_ref
    q2 = list(
        comp_coll.aggregate(
            [
                {
                    "$lookup": {
                        "from": "persons_emb",
                        "localField": "_id",
                        "foreignField": "company._id",
                        "as": "employees",
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "name": 1,
                        "num_employees": {"$size": "$employees"},
                    }
                },
                {"$sort": {"num_employees": -1}},
            ]
        )
    )
    t2 = time.time() - start
    print(f"Q2: {len(q2)} companies, time={t2:.4f}s")
    print("Q2 sample:", q2[:3])

    # Q3: update ages for those born before 1988-01-01
    start = time.time()
    result3 = pers_coll.update_many(
        {"birth_date": {"$lt": datetime(1988, 1, 1)}}, {"$set": {"age": 30}}
    )
    t3 = time.time() - start
    print(f"Q3: modified {result3.modified_count} docs, time={t3:.4f}s")

    # Q4: append ' Company' to company names (only if not already present)
    start = time.time()
    result4 = pers_coll.update_many(
        {"company.name": {"$not": {"$regex": " Company$"}}},
        [{"$set": {"company.name": {"$concat": ["$company.name", " Company"]}}}],
    )
    t4 = time.time() - start
    print(f"Q4: modified {result4.modified_count} docs, time={t4:.4f}s")


def run_queries_M3(db: Database) -> None:
    """
    Execute demonstration queries against Model 3 (company-embedded employees).
    """
    comp_coll = db.companies_emb
    print("\n=== M3 Queries ===")

    # Q1: unwind employees to get full names
    start = time.time()
    q1 = list(
        comp_coll.aggregate(
            [
                {"$unwind": "$employees"},
                {
                    "$project": {
                        "full_name": {
                            "$concat": [
                                "$employees.first_name",
                                " ",
                                "$employees.last_name",
                            ]
                        },
                        "company_name": "$name",
                        "_id": 0,
                    }
                },
            ]
        )
    )
    t1 = time.time() - start
    print(f"Q1: {len(q1)} docs, time={t1:.4f}s")
    print("Q1 sample:", q1[:3])

    # Q2: count employees per company (including companies with 0 employees)
    start = time.time()
    q2 = list(
        comp_coll.aggregate(
            [
                {
                    "$project": {
                        "name": 1,
                        "num_employees": {"$size": "$employees"},
                        "_id": 0,
                    }
                },
                {"$sort": {"num_employees": -1}},
            ]
        )
    )
    t2 = time.time() - start
    print(f"Q2: {len(q2)} companies, time={t2:.4f}s")
    print("Q2 sample:", q2[:3])

    # Q3: update embedded ages for those born before 1988-01-01
    start = time.time()
    result3 = comp_coll.update_many(
        {"employees.birth_date": {"$lt": datetime(1988, 1, 1)}},
        {"$set": {"employees.$[e].age": 30}},
        array_filters=[{"e.birth_date": {"$lt": datetime(1988, 1, 1)}}],
    )
    t3 = time.time() - start
    print(f"Q3: modified {result3.modified_count} docs, time={t3:.4f}s")

    # Q4: append ' Company' to company names (only if not already present)
    start = time.time()
    result4 = comp_coll.update_many(
        {"name": {"$not": {"$regex": " Company$"}}},
        [{"$set": {"name": {"$concat": ["$name", " Company"]}}}],
    )
    t4 = time.time() - start
    print(f"Q4: modified {result4.modified_count} docs, time={t4:.4f}s")


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

def get_mongo_client(uri: str) -> MongoClient:
    """
    Create a tuned *pymongo* client for high-concurrency workloads.

    :param uri: MongoDB connection string.
    :return: Configured MongoClient.
    """
    return MongoClient(
        uri,
        maxPoolSize=50,
        waitQueueTimeoutMS=2500,
        retryWrites=True,
    )


def populate_data(
    db: Database,
    fake: Faker,
    n_companies: int,
    n_persons: int,
) -> None:
    """
    High-level entry point that chooses the optimized or standard population
    path based on dataset size, then calls the M2 / M3 builders.

    :param db: Target MongoDB database.
    :param fake: Faker instance.
    :param n_companies: Number of companies to generate.
    :param n_persons: Number of persons to generate.
    """
    # Threshold values chosen empirically.
    if n_companies > 10_000 or n_persons > 100_000:
        print("Using optimized batch processing for large dataset...")

        company_ids = generate_and_populate_companies_batched(
            db,
            fake,
            n_companies,
            batch_size=min(50_000, max(1, n_companies // 100)),
        )
        generate_and_populate_persons_batched(
            db,
            fake,
            n_persons,
            company_ids,
            batch_size=min(50_000, max(1, n_persons // 1_000)),
        )
        print("M1 already populated during batch processing")

        populate_M2_optimized(db, batch_size=25_000)
        populate_M3_optimized(db, batch_size=2_500)
    else:
        print("Using standard processing for small dataset...")
        companies: List[Dict[str, Any]] = []

        batch_size_companies = min(10_000, n_companies)
        temp_coll = db["__temp_comp"]
        temp_coll.drop()

        for i in range(0, n_companies, batch_size_companies):
            batch_count = min(batch_size_companies, n_companies - i)
            company_batch = generate_companies(fake, batch_count)
            batch_ids = temp_coll.insert_many(company_batch).inserted_ids

            for comp, _id in zip(company_batch, batch_ids):
                comp["_id"] = _id
            companies.extend(company_batch)

            print(
                f"Generated company batch {i // batch_size_companies + 1}/"
                f"{(n_companies - 1) // batch_size_companies + 1}"
            )

        temp_coll.drop()
        comp_ids = [c["_id"] for c in companies]

        print(f"Generating {n_persons} persons...")
        persons: List[Dict[str, Any]] = []
        batch_size_persons = min(50_000, n_persons)

        for i in range(0, n_persons, batch_size_persons):
            batch_count = min(batch_size_persons, n_persons - i)
            person_batch = generate_persons(fake, batch_count, comp_ids)
            persons.extend(person_batch)
            print(
                f"Generated person batch {i // batch_size_persons + 1}/"
                f"{(n_persons - 1) // batch_size_persons + 1}"
            )

        print(
            f"Generated {len(companies)} companies and {len(persons)} persons."
        )

        batch_size_insert = 50_000 if n_persons > 100_000 else 100_000
        populate_M1(db, companies, persons, batch_size_insert)
        populate_M2(db, companies, persons, batch_size_insert)
        populate_M3(db, companies, persons, min(5_000, max(1, len(companies) // 10)))

def run_queries(db: Database) -> None:
    """
    Convenience wrapper that runs all three model query suites.
    """
    run_queries_M1(db)
    run_queries_M2(db)
    run_queries_M3(db)

def main() -> None:
    """
    CLI front-end for data generation and query benchmarking.
    """
    parser = argparse.ArgumentParser(description="Lab1 MongoDB: populate or run queries")
    sub = parser.add_subparsers(dest="action", required=True)

    pop = sub.add_parser("populate", help="Create and populate collections")
    pop.add_argument("--uri", type=str, default=os.getenv("MONGODB_URI", "mongodb://localhost:27017"))
    pop.add_argument("--db", type=str, default="lab1_db")
    pop.add_argument("--n_companies", type=int, default=5)
    pop.add_argument("--n_persons", type=int, default=50_000)

    qry = sub.add_parser("queries", help="Run queries on existing data")
    qry.add_argument("--uri", type=str, default=os.getenv("MONGODB_URI", "mongodb://localhost:27017"))
    qry.add_argument("--db", type=str, default="lab1_db")

    args = parser.parse_args()
    client = get_mongo_client(args.uri)
    db = client[args.db]

    if args.action == "populate":
        fake = Faker()
        populate_data(db, fake, args.n_companies, args.n_persons)
    else:
        run_queries(db)


if __name__ == "__main__":
    main()
