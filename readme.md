# Lab2 MongoDB Models

**Group Members:** Aleksandr Smolin, Denis Shadrin

A sample project demonstrating three data modeling approaches in MongoDB (referenced, embedding one-to-many, embedding many-to-one) using Python, Faker, and PyMongo. The script supports two modes:

* **populate** – generate fake data and populate three models (M1, M2, M3)
* **queries** – run four example queries/updates (Q1–Q4) on existing data

---

## 🛠️ Prerequisites

* Python 3.8+
* MongoDB (local or remote)
* Dependencies (install via pip):

  ```bash
  pip install pymongo faker
  ```

## 🚀 Getting Started

1. **Clone the repository**

   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Configure MongoDB URI**

   * By default, the script uses `mongodb://localhost:27017`.
   * To connect to a remote instance or use credentials, set the `MONGODB_URI` environment variable or pass `--uri`.

3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

## ⚡ Usage

### 1. Populate collections

Generate fake data and create three models:

```bash
python lab1_mongodb_models.py populate \
  --uri mongodb://localhost:27017 \
  --db lab1_db \
  --n_companies 4999 \
  --n_persons 50001
```

* `--db`: database name (default: `lab1_db`)
* `--n_companies`: number of company documents
* `--n_persons`: number of person documents

### 2. Run queries

Execute Q1–Q4 on existing data:

```bash
python lab1_mongodb_models.py queries \
  --uri mongodb://localhost:27017 \
  --db lab1_db
```

This will print counts, timings, and samples for each query in each model.

## 📂 Clean Up

To drop the database (remove all data):

```bash
python clean_db.py \
  --uri mongodb://localhost:27017 \
  --db lab1_db
```

## 🔍 Project Structure

```
├── lab1_mongodb_models.py    # Main script (populate & queries)
├── clean_db.py               # Script to drop a MongoDB database
├── README.md                 # This readme
└── requirements.txt          # Python dependencies
```
