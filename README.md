## Spark ETL Workflow(Ecomm Data Analysis)

This repository contains a set of Spark ETL workflows designed to extract, transform, and load data using PySpark.

### Tech Stack

- Python 3.11
- PySpark
- Docker

### Project Structure

- `main.py`: Main script to run Spark ETL workflows.
- `workflows.py`: Contains classes defining different ETL workflows.
- `src/`: Directory containing modules for extraction, transformation, and loading.
    - `extractor.py`: Defines data extraction logic.
    - `reader.py`: Defines reading data from different sources logic.
    - `transformer.py`: Defines data transformation logic.
    - `loader.py`: Defines data loading logic.
    - `spark_context.py`: Initializes the Spark session.

### Usage

1. Make sure you have Docker installed on your machine.
2. Build the Docker image:

    ```
    docker build -t my-spark-app .
    ```

3. Run a specific workflow using the Docker image:

    ```
    docker run --rm my-spark-app <WorkflowName>
    ```

Replace `<WorkflowName>` with the name of the workflow you want to run (e.g., `FirstWorkflow`, `SecondWorkflow`, etc.).
