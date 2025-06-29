# MongoDB Database Transfer Tool

Simple Python script to transfer entire MongoDB databases between servers with resume capability and duplicate prevention.

## Features

* Transfer entire database (all collections automatically)
* Resume from interruption point
* Prevent duplicate data
* Progress tracking with progress bars
* Batch processing (500 documents per batch)
* Connection retry logic

## Installation

```bash
pip install -r requirements.txt
```

## Usage

1. Update MongoDB URIs in the script
2. Run the script:

```bash
python mongo_transfer.py
```

3. Choose transfer method:
   * Option 1: Transfer entire database
   * Option 2: Resume from specific collection/document

## Configuration

Edit these lines in the script:

```python
source_uri = "mongodb://source_user:source_password@source_host:27017/admin"
destination_uri = "mongodb://dest_user:dest_password@dest_host:27017/admin"
```

## Requirements

* Python 3.6+
* MongoDB source and destination servers
* Network connectivity between servers

## Resume Transfer

If transfer stops, run option 2 and specify:

* Collection name to resume from
* Document number to resume from

The script automatically detects existing data and prevents duplicates.
