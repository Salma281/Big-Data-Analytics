## Name: Salma Ahmed Elanany
## ID: 231006903
## Class: AI2  Sat 12:30
--------------------
 
# MapReduce Lab

A small Flask-based MapReduce demonstration that processes text and numeric datasets using a parallel map/reduce workflow. The app lets you upload one or more `.txt` files, choose an operation, and inspect the pipeline step by step.

## Overview

This project simulates a simplified distributed data-processing workflow with:

- Map phase
- Combiner phase
- Partitioning across reducers
- Shuffle and sort
- Reduce phase
- Result output files in the `output/` folder

It is designed as a teaching/demo project for understanding how MapReduce works without relying on Hadoop or Spark.

## Features

- Text analytics:
  - Word count
  - Top 10 most frequent words
- Numeric analytics:
  - Sum
  - Count
  - Minimum
  - Maximum
  - Average
- Batch upload of text files
- Browser-based UI with multiple result views:
  - Simple result view
  - Detailed processing view
  - Flow diagram view
- Parallel processing using Python's `ProcessPoolExecutor`

## Project Structure

```text
.
├── app.py               # Flask web application
├── engine.py            # MapReduce implementation
├── requirements.txt     # Python dependencies
├── output/              # Generated reducer output files
├── templates/           # Jinja2 HTML templates
│   ├── base.html
│   ├── index.html
│   ├── result.html
│   └── flow.html
├── input/               # Input for(Text analytics MapReduce)
├── input2/              # Additional input for(Text analytics MapReduce)
├── input_sales/         # Numeric sample input
└── README.md            # Project documentation
```

## Requirements

- Python 3.9+
- Flask

## Setup

1. Open a terminal in the project root.
2. Create and activate a virtual environment (optional but recommended):

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

## Run the Application

```bash
python app.py
```

Then open the app in your browser:

```text
http://localhost:5000
```

## How to Use

1. Upload a folder containing one or more `.txt` files.
2. Select the processing type:
   - `Text analytics MapReduce`
   - `Numeric MapReduce`
3. Choose an operation from the dropdown.
4. Pick a result view:
   - Simple
   - Detail
   - Flow
5. Click `Run job`.

## Numeric Input Format

For numeric operations, each non-empty line should contain a key and a number, such as:

```text
Chai,3
Tea 5
Coffee: 2.5
```

The app accepts values in formats like:

- `key,value`
- `key value`
- `key: value`
- `key; value`

## Notes

- Only `.txt` files are processed.
- Each uploaded file becomes one map split.
- Results are written to files named like `part-00000` and `part-00001` inside the `output/` directory.
- The application stores per-session job state in memory while the browser session remains active.

## Example Use Cases

- Count word frequencies in multiple documents
- Aggregate sales or product metrics by category
- Analyze repeated terms across text batches
- Demonstrate map/reduce logic in a classroom or lab setting


