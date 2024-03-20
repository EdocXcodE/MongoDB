# MongoDB Local Instance Setup using Python Driver

This repository contains Python scripts for interacting with a locally hosted MongoDB instance. The provided scripts facilitate connecting to the MongoDB server, loading the `sample_mflix` database, and solving queries related to comments, movies, and theaters.

## Requirements

- [MongoDB](https://www.mongodb.com/try/download/community) installed locally
- [Python 3](https://www.python.org/downloads/) installed on your machine

## Setup

1. **Clone the Repository**: Clone this repository to your local machine:

    ```bash
    git clone <repository_url>
    ```

2. **Navigate to Repository Directory**: Move into the cloned repository directory:

    ```bash
    cd <repository_directory>
    ```

3. **Install Dependencies**: Install the required Python libraries listed in `requirements.txt`:

    ```bash
    pip install -r requirements.txt
    ```

4. **Connect to MongoDB Server**: Use `connectServer.py` to connect to your local MongoDB instance. Ensure MongoDB server is running.

    ```bash
    python connectServer.py
    ```

5. **Load Sample Database**: Run `create-load-database.py` to load the `sample_mflix` dataset into your MongoDB database.

    ```bash
    python create-load-database.py
    ```

## Usage

- **commentsSolution.py**: Contains methods to solve MongoDB queries related to comments section.
- **moviesSolution.py**: Contains methods to solve MongoDB queries related to movies section.
- **theatreSolution.py**: Contains methods to solve MongoDB queries related to theaters section.


