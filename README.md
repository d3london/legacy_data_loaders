# Legacy data loading (AIC/SDE)

Bash scripts used to clean and load legacy data (CSV) into Postgres db running in a Docker container.

As part of the London SDE Programme, the London AI Centre is working in NHS Hospital Trusts to perform ELT and standardisation of Electronic Health Record (EHR) data. Trusts may hold as much as 20 years of legacy data - either in originating systems, in long term archival storage, or in data warehouses.

## Infrastructure

Default infrastructure is an on-prem Linux server, part of an Nvidia DGX Platform used by the AI Centre for training, deployment, and federation across multiple sites.  

## Pipeline

Load of legacy data follows the below process. Note that 'live' data, and unstructured data, follow different pipelines.

```mermaid
  graph TD
      A["Legacy systems"]-->|"CSV export + compression"|B["Staging directory"]
      B-->|"unzip_parse_csv.sh"|C["CSV ready for load"]
      C-->|"load_csv_to_postgres.sh"|D["Load into db.source schema"]
```

Note that:
- Legacy CSVs are expected to be exported with UTF-8 encoding and pipe delimiter;
- Incoming compressed CSVs (.zip) are staged in ```/srv/shared/```;
- CSVs are unzipped into ```/srv/shared/sources``` for load;
- Database connection details should be configured as environmental variables
 
## Usage

1. Set environmental variables, e.g.:
```
$ echo 'export CONTAINER_NAME="omop_postgres"' >> ~/.bashrc
$ echo 'export DATABASE_NAME="omop_dev"' >> ~/.bashrc
$ source ~/.bashrc
```

2. Select compressed CSV file to unzip and parse delimiters/newline/encoding: 
```
$ ./unzip_parse_csv.sh
```

3. Select CSV file to load into Postgres:
```
$ ./load_csv_to_postgres.sh
```

<a href="https://www.aicentre.co.uk/"><img src="logo_aic.png" alt="London AI Centre" title="" height="70" /></a>