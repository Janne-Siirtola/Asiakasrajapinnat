# Asiakasrajapinnat

Asiakasrajapinnat is a collection of Azure Functions that process and deliver customer data files. The solution is primarily composed of a timer triggered pipeline that reads raw CSV data from Azure Blob Storage, normalises and validates it and finally publishes the result both to another storage container and to an Azure SQL database. In addition the project contains an HTTP based configuration UI used for managing customer specific settings.

## Repository layout

```
asiakasrajapinnat_master/   # Core pipeline and helpers
config_page/                # HTTP endpoint for managing configuration
Config/                     # JSON configuration files
local_tests/                # Scripts for running the pipeline locally
tests/                      # Pytest based unit tests
```

### Timer triggered pipeline (`asiakasrajapinnat_master`)

The timer triggered function loads customer definitions from the `Config` container in Azure Storage. For each customer it fetches the newest CSV file from the configured source container, runs a series of cleaning and validation steps (implemented in `data_editor.py`) and writes the cleaned records into an Azure SQL table. The pipeline then builds the final output file in CSV or JSON format using `data_builder.py` and uploads it back to storage together with an ESRS report in JSON format.

### Configuration page (`config_page`)

The HTTP endpoint provides a simple web interface for editing customer configurations and the list of base columns. The UI is rendered with Jinja templates and includes CSRF protection. Form submissions are validated and stored as JSON files inside the configuration container.

### Local testing

The `local_tests/run_workflow.py` script demonstrates how to execute the pipeline locally. It reads configuration files from the `Config` directory and expects a `local.settings.json` file for environment variables such as database credentials and storage connection strings.

## Running tests

Install the required packages and run the test suite with `pytest`:

```bash
pip install -r requirements.txt
pytest
```

The tests cover data processing helpers, storage and database integrations as well as the configuration utilities.