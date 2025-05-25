
# Lab2 Big Data Management MongoDB Models

**Group Members:** Aleksandr Smolin, Denis Shadrin

This project demonstrates three MongoDB data modeling approaches (M1: referenced, M2: embedded company in person, M3: embedded employees in company) using Python, Faker, and PyMongo. The script is optimized for both small and very large datasets (hundreds of thousands to millions of documents) and supports two modes:

* **populate** – generate fake data and populate all three models using memory-efficient batch processing
* **queries** – run four example queries/updates (Q1–Q4) on existing data with performance measurement

---

##  Prerequisites

* Python 3.8+
* MongoDB (local or remote)
* Dependencies:
  ```bash
  pip install pymongo faker
  ```

##  Getting Started

1. **Configure MongoDB URI**
   * Default: `mongodb://localhost:27017`
   * Custom: Set `MONGODB_URI` environment variable or use `--uri` parameter

3. **Install dependencies**
   ```bash
   pip install pymongo faker
   ```

##  Usage

### 1. Populate collections (auto-optimized for dataset size)

**Small dataset (in-memory, fast):**
```bash
python main.py populate --db lab1_db --n_companies 1000 --n_persons 10000
```

**Large dataset (streaming batch mode):**
```bash
python main.py populate --db lab1_db --n_companies 500000 --n_persons 5000000
```

* The script automatically switches to optimized batch mode for >10,000 companies or >100,000 persons.
* You can use `--uri` to specify a remote MongoDB instance.

### 2. Run queries

Run Q1–Q4 on all three models:
```bash
python main.py queries --db lab1_db
```
This prints counts, timings, and samples for each query in each model.

### 3. Clean up

Drop the database (remove all data):
```bash
python clean_db.py --db lab1_db
```

##  Project Structure

```
├── main.py              # Main script (populate & queries)
├── clean_db.py          # Script to drop a MongoDB database
└── readme.md            
```

## Query Types

- **Q1**: Full name concatenation with company names
- **Q2**: Employee count per company 
- **Q3**: Age updates for employees born before 1988
- **Q4**: Append " Company" suffix to company names (if not present)
