#!/bin/bash

# AI Centre & London SDE
# CSV to Postgres loader for NHS Trust ELT

# Usage: $ ./load_csv_to_postgres.sh
# - User selects from CSV files in staging area (/srv/shared/sources/), must be UTD-8 and pipe delimited
# - User specifies new table name, if table already exists prompts to drop
# - Headers from CSV file are used to CREATE TABLE with all fields set as VARCHAR
# - CSV file is loaded into $DB_NAME.$SCHEMA.$TABLE_NAME

# Set database connection details

if [[ -z "$PGUSER" ]]; then
  echo 'Failure: PGUSER unset.'
  exit
fi

if [[ -z "$PGPASSWORD" ]]; then
  echo 'Failure: PGPASSWORD unset.'
  exit
fi

if [[ -z "$PGHOST" ]]; then
  echo 'Failure: PGHOST unset.'
  exit
fi

if [[ -z "$SCHEMA" ]]; then
  echo 'Info: SCHEMA unset. Assuming `source`'
  SCHEMA=source
fi

if [[ -z "$PGPORT" ]]; then
  echo 'Info: PGPORT unset. Assuming 5432'
fi

# Function: list CSV files and ask user to select one
select_csv_file() {
  echo "Available CSV files in /srv/shared/sources:"
  select FILE in /srv/shared/sources/*.csv; do
    if [ -n "$FILE" ]; then
      echo "You selected: $FILE"
      break
    else
      echo "Invalid selection. Please try again."
    fi
  done
}

# Function: test database connection
test_database_connection() {
  echo "Testing database connection:"
  psql -c "\dn"
  psql -c "\dt ${SCHEMA}.*"
}

# Function: get table name from user and check if it exists
get_table_name_and_check() {
  read -p "Enter the name of the target table: " TABLE_NAME

  if psql -tAc "SELECT 1 FROM pg_tables WHERE tablename = '${TABLE_NAME}' AND schemaname = 'source'" | grep -q 1; then
    echo "Table $SCHEMA.$TABLE_NAME already exists."
    read -p "Do you want to drop the existing table? (y/n): " confirm_drop
    if [ "$confirm_drop" = "y" ]; then
      psql -c "DROP TABLE $SCHEMA.$TABLE_NAME;" 2>&1
      if [ $? -ne 0 ]; then
        echo "Failed to drop the table. Exiting."
        exit 1
      fi
      echo "Table dropped."
    else
      echo "Operation cancelled."
      exit 1
    fi
  fi
}

# Function: read the first line of the CSV and pass column names to variable CSV_HEADERS
parse_csv_header() {
  local csv_file="$1"
  IFS='|' read -ra CSV_HEADERS <"$csv_file"
}

# Function: generate and execute the CREATE TABLE statement
create_table() {
  local table_name="$1"
  shift
  local -a local_columns=("$@")

  local create_statement="CREATE TABLE $SCHEMA.$table_name ("

  # Add all columns from CSV headers
  for column in "${local_columns[@]}"; do
    create_statement+="\"$column\" VARCHAR,"
  done

  # Add additional columns
  create_statement+="source_row_uuid UUID DEFAULT gen_random_uuid(),"
  create_statement+="source_table_provenance VARCHAR DEFAULT '$table_name'"
  create_statement+=")"

  echo "Executing CREATE TABLE statement:"
  echo "$create_statement"

  psql -c "$create_statement"

  if [ $? -ne 0 ]; then
    echo "Failed to create the table. Exiting."
    exit 1
  fi
  echo "Table created successfully."
}

# Function: batch csv data
echo "Loading data from $csv_file into $SCHEMA.$table_name in batches..."

# Function: load CSV data into the specified table
load_csv_data() {
  local csv_file="$1"
  local table_name="$2"
  shift 2
  local -a columns=("$@")

  echo "Loading data from $csv_file into $SCHEMA.$table_name..."

  # Build the column list string
  local column_list=""
  for column in "${columns[@]}"; do
    column_list+="\"$column\","
  done
  # Remove the trailing comma
  column_list=${column_list%,}

  local copy_statement="\\COPY $SCHEMA.$table_name($column_list) FROM STDIN WITH (FORMAT csv, QUOTE E'\\x01', DELIMITER '|', HEADER true, ENCODING 'UTF8')"

  # Use cat to read the file and pipe to psql
  cat "$csv_file" | psql -c "$copy_statement"

  if [ $? -ne 0 ]; then
    echo "Failed to load data into the table. Exiting."
    exit 1
  fi
  echo "Data loaded successfully."
}

# Function: batch process the CSV file
load_csv_data_in_batches() {
  local csv_file="$1"
  local table_name="$2"
  shift 2
  local -a columns=("$@")

  echo "Loading data from $csv_file into $SCHEMA.$table_name in batches..."

  # Set batch size
  local batch_size=1000000

  # Create a temporary directory for chunk files
  local temp_dir=$(mktemp -d)

  # Extract header
  head -n 1 "$csv_file" >"$temp_dir/header.csv"

  # Process file in batches
  awk -v batch_size="$batch_size" -v temp_dir="$temp_dir" '
    BEGIN { 
        FS = "|"  # Set field separator to pipe
        batch = 1
        count = 0
        file = temp_dir "/chunk_" batch ".csv"
        print "Processing batch", batch
    }
    NR == 1 { next }  # Skip header
    {
        if (count == 0) {
            system("cp " temp_dir "/header.csv " file)
        }
        print $0 >> file
        count++
        if (count >= batch_size) {
            close(file)
            count = 0
            batch++
            file = temp_dir "/chunk_" batch ".csv"
            print "Processing batch", batch
        }
    }
    END {
        if (count > 0) {
            close(file)
        }
        print "Total batches:", batch
    }
    ' "$csv_file"

  # Load each batch
  for chunk in "$temp_dir"/chunk_*.csv; do
    echo "Loading batch: $(basename "$chunk")"
    load_csv_data "$chunk" "$table_name" "${columns[@]}"
    rm "$chunk"
  done

  # Clean up temporary directory
  rm -r "$temp_dir"

  echo "All batches processed successfully."
}

# Execute
select_csv_file
test_database_connection
get_table_name_and_check
parse_csv_header "$FILE"
create_table "$TABLE_NAME" "${CSV_HEADERS[@]}"
load_csv_data_in_batches "$FILE" "$TABLE_NAME" "${CSV_HEADERS[@]}"
echo "Script execution completed."
