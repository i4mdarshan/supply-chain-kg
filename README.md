# Supply Chain Risk Intelligence — Knowledge Graph

> DSCI 558: Building Knowledge Graphs — USC, Spring 2026
> Team: Jash Prakash Shah & Darshan Nilesh Mahajan

This project builds a knowledge graph of global supply chain disruptions, integrating news events, sanctions data, and international trade records. The graph enables financial analysts to identify supplier exposure, trace disruption propagation, and query risk relationships in natural language.

## Prerequisites

- **Python 3.11 or higher** — check with `python --version`
- **Git** — check with `git --version`
- **A terminal** — Terminal on Mac/Linux, Git Bash or PowerShell on Windows

---

## Step 1 — Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/supply-chain-kg.git
cd supply-chain-kg
```

Replace `YOUR_USERNAME` with the actual GitHub username where the repo lives.

---

## Step 2 — Create a virtual environment

A virtual environment keeps this project's dependencies isolated from the rest of your system. Think of it as a clean room just for this project.

**On Mac / Linux:**

```bash
python -m venv .venv
source .venv/bin/activate
```

**On Windows (Git Bash):**

```bash
python -m venv .venv
source .venv/Scripts/activate
```

You should now see `(.venv)` at the start of your terminal prompt. This means the virtual environment is active.

> **Every time you open a new terminal window**, you need to activate the environment again with the `source` command above before running any project code.

---

## Step 3 — Install dependencies

```bash
pip install -r requirements.lock
```

This installs all project dependencies defined in `requirements.lock`.

This will take 2–4 minutes the first time. You should see packages being downloaded and installed.

**After installation, also download the spaCy language model:**

```bash
python -m spacy download en_core_web_lg
```

> If you ever add a new package to `pyproject.toml`, re-run `pip install -e ".[dev]"` and then regenerate `requirements.lock`.

---

## Step 4 — Set up environment variables

The project uses API keys and database credentials that should never be committed to Git. A template file is provided.

```bash
cp .env.example .env
```

---

## Step 5 — Verify the setup

Run this to confirm everything is installed correctly:

```bash
python -c "import neo4j, spacy, pandas, rdflib, rapidfuzz; print('All good!')"
```

You should see `All good!` with no errors.

---

## Project structure overview

```
supply-chain-kg/
├── src/
│   ├── acquisition/        # Data downloaders (one file per source)
│   │   ├── base.py         # Abstract base class all sources inherit
│   │   ├── gdelt.py        # GDELT GKG bulk downloader
│   │   ├── newsapi.py      # NewsAPI supplementary pulls
│   │   ├── ofac.py         # OFAC SDN XML parser
│   │   └── eu_sanctions.py # EU sanctions JSON parser
│   ├── extraction/         # NER pipeline and field parsers
│   ├── resolution/         # Entity resolution (string + semantic + spatial)
│   ├── ingestion/          # Neo4j loader and RDF exporter
│   ├── analysis/           # Risk scoring, propagation, link prediction
│   ├── queries/            # Cypher and SPARQL query templates
│   ├── api/                # FastAPI layer (serves Streamlit + future Next.js)
│   └── app/                # Streamlit pages (course deliverable)
├── data/                   # Gitignored — local only
│   ├── raw/                # Downloaded source files (one folder per source)
│   ├── processed/          # Cleaned staging parquet files
│   ├── ontology/           # OWL/Turtle ontology definitions
│   └── exports/            # RDF export for SPARQL grading
├── tests/                  # Pytest test suite
├── notebooks/              # Exploration notebooks (not production code)
├── docs/                   # Architecture notes and data dictionary
├── pyproject.toml          # Python dependencies
├── requirements.lock       # Pinned versions for reproducibility
├── .env.example            # API key template (safe to commit)
└── .gitignore
```
