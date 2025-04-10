# 📊 PySpark Campaign Analyzer

[![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=fff)](https://www.docker.com/)
[![Python Version](https://img.shields.io/badge/python-^3.09-blue?logo=python&logoColor=fff.svg)](https://www.python.org/downloads/release/python-3100/)
[![Poetry](https://img.shields.io/badge/dependencies-poetry-purple.svg)](https://python-poetry.org/)
[![Spark](https://img.shields.io/badge/spark-3.5.5-orange)](https://spark.apache.org/)

---
Analyze marketing campaign performance at scale using PySpark, with auto-generated test data and full Docker support.
Identify inefficient campaigns based on cost-per-conversion and view results directly in your logs.
---

## 🚀 Features

- ✅ Synthetic data generation
- ✅ PySpark analysis pipeline
- ✅ Performance reporting via stdout & generated local CSVs
- ✅ Dependency management via Poetry
- ✅ Dockerized

---

## ⚙️ Setup & Run

### 1. Clone the repo

```bash
git clone https://github.com/sebboswift/campaign_analysis.git
cd campaign_analysis
```

### 2. Build & run with Docker-Compose

```bash
docker-compose up --build
```

This will (by default):

- Generate 1.5M+ rows of campaign data (CSV ~100MB) in `gen_data/synthetic`
- Run the PySpark job
- Output a preview of inefficient campaigns (`cost_per_conversion_eur > 10`) directly in the logs
- Yield a folder containing a CSV with all inefficient campaigns for further analysis in `gen_data/reporting`

---

## ✨ Customization

You can adjust some of the business logic in `src/config.py`:

```python
# The amount of mock-data rows to synthesize
NUM_SYNTHETIC_ROWS = 1_500_000

# Cost-Per-Conversion (CPC) inefficiency threshold (EUR per conversion)
CPC_INEFFICIENCY_THRESHOLD_EUR = 10.0

# Max. percentage of inefficient campaigns before sending an alert
MIN_ALERTING_INEFFICIENCY_PERCENTAGE = 2.5

# Whether you want all campaign results or just the inefficient one's
STORE_ONLY_INEFFICIENT_CAMPAIGNS = True
```

---

## 🧑‍💻 Local development (optional)

### 1. Dependencies via Poetry
**Make sure to install Poetry and the dependencies before proceeding with either this setup or the tests.**

Poetry is the lovely way we manage our dependencies with. You can install [Poetry](https://python-poetry.org/)
via [pip](https://pypi.org/project/pip/):

```bash
pip install poetry
```

and then use it like this to install the dependencies:

```bash
poetry install
```
### 2. OpenJDK for PySpark

The tests in this repo rely on PySpark, which depends on a compatible OpenJDK version on your machine. To install one via [Homebrew](https://brew.sh/) run:

```bash
brew install openjdk@11
```

### 3. Pre-commit

This project supports [pre-commit hooks](https://pre-commit.com/). To set them up in your `.git/` directory, use:

```bash
poetry run pre-commit install
```
---
## 🧪 Testing (optional)
You may have to adjust the PATH of Java in your session before running tests:
```bash
export JAVA_HOME="$($(which brew) --prefix openjdk@11)/libexec/openjdk.jdk/Contents/Home"
```
Then you can safely run the tests using PyTest:
```bash
poetry run python -m pytest src/tests
```
If you'd like to measure the test coverage of the `src/` module you instead run:
```bash
poetry run python -m pytest --cov=src src/tests
```
---
## ⚖️ License

MIT No Attribution – do whatever you want 🙌
