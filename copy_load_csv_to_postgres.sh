#!/bin/bash

# AI Centre & London SDE
# CSV to Postgres loader for NHS Trust ELT

# Set database connection details
CONTAINER_NAME="${CONTAINER_NAME:-omop_postgres}"  # env variable, or default to "omop_postgres"
DATABASE_NAME="${DATABASE_NAME:-omop_dev}"  # env variable, or default to "omop_dev"
PG_USER="$PG_USER" # env variable
PG_PASSWORD="$PG_PASSWORD" # env variable

# Path to Psql statement for container
PSQL="docker exec -i $CONTAINER_NAME psql -U $PG_USER -d $DATABASE_NAME"

# Directory containing the raw CSV files
SOURCE_DIR="/srv/shared/sources/"

# Mounted directory for postgres docker container
DEST_DIR="/var/lib/postgresql/data/sources/"

# Function: list CSV files and ask user to select one
select_csv_file() {
    echo "Available CSV files in $SOURCE_DIR:"
    select FILE in $SOURCE_DIR*.csv; do
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

# Function: read the first line of the CSV and pass column names to variable CSV_HEADERS  
parse_csv_header() {
    local csv_file="$1"
    IFS='|' read -ra CSV_HEADERS < "$csv_file"
}

# Function: generate and execute the CREATE TABLE statement
create_table() {
    local table_name="$1"
    shift
    local -a columns=("$@")

    local create_statement="CREATE TABLE source.$table_name ("
    
    # Add all columns from CSV headers
    for column in "${columns[@]}"; do
        create_statement+="\"$column\" VARCHAR,"
    done

    # Add additional columns
    create_statement+="source_row_uuid UUID DEFAULT uuid_generate_v4(),"
    create_statement+="source_table_provenance VARCHAR DEFAULT '$table_name'"
    create_statement+=")"
    
    echo "Executing CREATE TABLE statement:"
    echo "$create_statement"
    
    $PSQL -c "$create_statement"

    if [ $? -ne 0 ]; then
        echo "Failed to create the table. Exiting."
        exit 1
    fi
    echo "Table created successfully."
}

# Function: copy CSV file to Docker container
copy_file_to_docker() {
    local source_file="$1"
    local filename=$(basename "$source_file")

    # Check if the dir exists already, create if doesn't
    docker exec $CONTAINER_NAME bash -c "[ -d $DEST_DIR ] || mkdir -p $DEST_DIR"

    echo "Copying $filename to the container..."
    docker cp "$source_file" "$CONTAINER_NAME:$DEST_DIR"

    if [ $? -ne 0 ]; then
        echo "Failed to copy file to Docker container. Exiting."
        exit 1
    fi
    echo "File copied successfully."
}

# Function: load CSV data into the specified table
load_csv_data() {
    local table_name="$1"
    local filename="$2"
    shift 2
    local -a columns=("$@")
    
    echo "Loading data from $filename into source.$table_name..."
    
    ###local copy_statement="COPY source.$table_name("    
    local copy_statement="\copy source.$table_name("
    
    # Add all columns from CSV headers
    for column in "${columns[@]}"; do
        copy_statement+="\"$column\","
    done

    # Remove the trailing comma and close the parenthesis
    copy_statement=${copy_statement%,}
    copy_statement+=") FROM '$DEST_DIR$filename' WITH (FORMAT csv, DELIMITER '|', HEADER true, ENCODING 'UTF8')"
    
    echo "Executing \copy command:"
    echo "$copy_statement"
    
    $PSQL -c "$copy_statement"
    
    if [ $? -ne 0 ]; then
        echo "Failed to load data into the table. Exiting."
        exit 1
    fi
    echo "Data loaded successfully."
}

# Function: verify data load
verify_data_load() {
    local table_name="$1"
    echo "Verifying data load..."
    local row_count=$($PSQL -tAc "SELECT COUNT(*) FROM source.$table_name")
    echo "Rows loaded into source.$table_name: $row_count"
}

# Function: delete file from Docker container
delete_file_from_docker() {
    local filename="$1"
    echo "Deleting $filename from Docker container..."
    docker exec $CONTAINER_NAME rm "$DEST_DIR$filename"
    if [ $? -ne 0 ]; then
        echo "Failed to delete file from Docker container."
    else
        echo "File deleted successfully."
    fi
}

# Execute
select_csv_file
test_database_connection
get_table_name_and_check
parse_csv_header "$FILE"

create_table "$TABLE_NAME" "${CSV_HEADERS[@]}"

copy_file_to_docker "$FILE"
load_csv_data "$TABLE_NAME" "$(basename "$FILE")" "${CSV_HEADERS[@]}"

verify_data_load "$TABLE_NAME"
delete_file_from_docker "$(basename "$FILE")"

echo "Script execution completed."