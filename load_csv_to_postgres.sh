#!/bin/bash

# AI Centre & London SDE
# CSV to Postgres loader for NHS Trust ELT

# Usage: $ ./load_csv_to_postgres.sh
# - User selects from CSV files in staging area (/srv/shared/sources/), must be UTD-8 and pipe delimited 
# - User specifies new table name, if table already exists prompts to drop
# - Headers from CSV file are used to CREATE TABLE with all fields set as VARCHAR
# - CSV file is loaded into $DB_NAME.source.$TABLE_NAME


# Set database connection details
CONTAINER_NAME="${CONTAINER_NAME:-omop_postgres}"  # env variable, or default to "omop_postgres"
DATABASE_NAME="${DATABASE_NAME:-omop_dev}"  # env variable, or default to "omop_dev"
PG_USER="$PG_USER" # env variable
PG_PASSWORD="$PG_PASSWORD" # env variable

# Path to Psql statement for container
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

# Function: read the first line of the CSV and pass column names to variable CSV_HEADERS  
parse_csv_header() {
    local csv_file="$1"
    IFS='|' read -ra CSV_HEADERS < "$csv_file"
}

# Function: generate and execute the CREATE TABLE statement
create_table() {
    local table_name="$1"
    shift
    local -a local_columns=("$@")

    local create_statement="CREATE TABLE source.$table_name ("
    
    for ((i=0; i<${#local_columns[@]}; i++)); do
        create_statement+="\"${local_columns[i]}\" VARCHAR"
        if [ $i -lt $((${#local_columns[@]} - 1)) ]; then
            create_statement+=","
        fi
    done
    
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

# Function: load CSV data into the specified table
load_csv_data() {
    local csv_file="$1"
    local table_name="$2"
    
    echo "Loading data from $csv_file into source.$table_name..."
    
    local copy_statement="\\COPY source.$table_name FROM STDIN WITH (FORMAT csv, DELIMITER '|', HEADER true, ENCODING 'UTF8')"
    
    # Use cat to read the file and pipe to psql
    cat "$csv_file" | docker exec -i $CONTAINER_NAME psql -U $PG_USER -d $DATABASE_NAME -c "$copy_statement"
    
    if [ $? -ne 0 ]; then
        echo "Failed to load data into the table. Exiting."
        exit 1
    fi
    echo "Data loaded successfully."
}

#load_csv_data() {
#   local csv_file="$1"
#   local table_name="$2"
#   shift 2
#   local -a local_columns=("$@")
#   
#   echo "Loading data from $csv_file into source.$table_name..."
#   
#   # Construct COPY statement
#   local copy_statement="\\COPY source.$table_name("
#   
#   for ((i=0; i<${#local_columns[@]}; i++)); do
#       copy_statement+="\"${local_columns[i]}\""
#       if [ $i -lt $((${#local_columns[@]} - 1)) ]; then
#           copy_statement+=","
#       fi
#   done
#   
#   copy_statement+=") FROM STDIN WITH (FORMAT csv, DELIMITER '|', HEADER true, ENCODING 'UTF8')"
#   
#   # Use cat to read the file and pipe to psql
#   cat "$csv_file" | docker exec -i $CONTAINER_NAME psql -U $PG_USER -d $DATABASE_NAME -c "$copy_statement"
#   
#   if [ $? -ne 0 ]; then
#       echo "Failed to load data into the table. Exiting."
#       exit 1
#   fi
#   echo "Data loaded successfully."
#}

# Execute
select_csv_file
test_database_connection
get_table_name_and_check
parse_csv_header "$FILE"
create_table "$TABLE_NAME" "${CSV_HEADERS[@]}"
load_csv_data "$FILE" "$TABLE_NAME" "${CSV_HEADERS[@]}"
echo "Script execution completed."