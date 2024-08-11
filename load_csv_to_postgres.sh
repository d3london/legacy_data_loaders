#!/bin/bash

# Check for GUID environment variable
echo "Debug: Checking for GUID environment variable"
if [ -n "$GUID" ]; then
    echo "GUID environment variable is set to: $GUID"
else
    echo "GUID environment variable is not set"
fi

# set postgres connection details
CONTAINER_NAME="omop_postgres"
DATABASE_NAME="omop_dev"
PG_USER="$PG_USER" #env variable
PG_PASSWORD="$PG_PASSWORD" #env variable

# Path to psql inside container
PSQL="docker exec -i $CONTAINER_NAME psql -U $PG_USER -d $DATABASE_NAME"

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
    $PSQL -c "\dn"
    $PSQL -c "\dt source.*"
}

# Function: get table name from user and check if it exists
get_table_name_and_check() {
    read -p "Enter the name of the target table: " TABLE_NAME

    if $PSQL -tAc "SELECT 1 FROM pg_tables WHERE tablename = '${TABLE_NAME}' AND schemaname = 'source'" | grep -q 1; then
        echo "Table source.$TABLE_NAME already exists."
        read -p "Do you want to drop the existing table? (y/n): " confirm_drop
        if [ "$confirm_drop" = "y" ]; then
            $PSQL -c "DROP TABLE source.$TABLE_NAME;" 2>&1
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

# Function: read the first line of the CSV and create an array of column names
parse_csv_header() {
    local csv_file="$1"
    IFS='|' read -ra COLUMNS < "$csv_file"
}

# Function: generate and execute the CREATE TABLE statement
create_table() {
    local table_name="$1"
    shift
    local -a local_columns=("$@")
    
    echo "Debug: create_table - Local columns at start:"
    printf '  - %s\n' "${local_columns[@]}"
    
    echo "Debug: create_table"
    echo "GLOBAL COLUMNS array content at start:"
    printf '  - %s\n' "${COLUMNS[@]}"
    
    local create_statement="CREATE TABLE source.$table_name ("
    
    echo "Debug: create_table"
    echo "GLOBAL COLUMNS array content after initial statement creation:"
    printf '  - %s\n' "${COLUMNS[@]}"
    
    for ((i=0; i<${#local_columns[@]}; i++)); do
        create_statement+="\"${local_columns[i]}\" VARCHAR"
        if [ $i -lt $((${#local_columns[@]} - 1)) ]; then
            create_statement+=","
        fi
    done
    
    echo "Debug: create_table"
    echo "GLOBAL COLUMNS array content after statement loop:"
    printf '  - %s\n' "${COLUMNS[@]}"
    
    create_statement+=")"
    
    echo "Executing CREATE TABLE statement:"
    echo "$create_statement"
    
    set -x
    $PSQL -c "$create_statement"
    set +x
    
    echo "Debug: create_table"
    echo "GLOBAL COLUMNS array content after psql -c create statement"
    printf '  - %s\n' "${COLUMNS[@]}"
    
    if [ $? -ne 0 ]; then
        echo "Failed to create the table. Exiting."
        exit 1
    fi
    echo "Table created successfully."
    
    echo "Debug: create_table - Local columns at end:"
    printf '  - %s\n' "${local_columns[@]}"
}

# Function: load CSV data into new table
load_csv_data() {
    local csv_file="$1"
    local table_name="$2"
    shift 2
    local -a local_columns=("$@")
    
    echo "Debug: load_csv_data - Local columns at start:"
    printf '  - %s\n' "${local_columns[@]}"
    
    echo "Loading data from $csv_file into source.$table_name..."
    
    # Construct COPY statement
    local copy_statement="\\COPY source.$table_name("
    
    for ((i=0; i<${#local_columns[@]}; i++)); do
        copy_statement+="\"${local_columns[i]}\""
        if [ $i -lt $((${#local_columns[@]} - 1)) ]; then
            copy_statement+=","
        fi
    done
    
    copy_statement+=") FROM STDIN WITH (FORMAT csv, DELIMITER '|', HEADER true, ENCODING 'UTF8')"
    
    echo "Debug: Full COPY statement:"
    echo "$copy_statement"
    
    # Use cat to read the file and pipe to psql
    cat "$csv_file" | docker exec -i $CONTAINER_NAME psql -U $PG_USER -d $DATABASE_NAME -c "$copy_statement"
    
    if [ $? -ne 0 ]; then
        echo "Failed to load data into the table. Exiting."
        exit 1
    fi
    echo "Data loaded successfully."
    
    echo "Debug: load_csv_data - Local columns at end:"
    printf '  - %s\n' "${local_columns[@]}"
}

# Execute main script
select_csv_file
test_database_connection
get_table_name_and_check
parse_csv_header "$FILE"

echo "Debug: After parse_csv_header"
echo "GLOBAL COLUMNS array content:"
printf '  - %s\n' "${COLUMNS[@]}"

create_table "$TABLE_NAME" "${COLUMNS[@]}"

load_csv_data "$FILE" "$TABLE_NAME" "${COLUMNS[@]}"

echo "Script execution completed."