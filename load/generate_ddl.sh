#!/bin/bash

# From a source CSV file, generates a postgres DDL for table creation
# Usage: 
# ./generate_ddl.sh /path/to/source.csv /path/to/destination.sql

# Check if the correct number of arguments was provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <source_csv_file> <destination_sql_file>"
    exit 1
fi

# Assign arguments to variables
CSV_FILE="$1"
SQL_FILE="$2"

# Check if csvkit is installed, install if not
if ! command -v csvsql &> /dev/null
then
    echo "csvsql could not be found, installing csvkit..."
    sudo apt update
    sudo apt install python3-pip -y
    sudo pip3 install csvkit
fi

# Generate SQL DDL from CSV
echo "Generating SQL DDL from CSV..."
csvsql --dialect postgresql --snifflimit 10000 "$CSV_FILE" > "$SQL_FILE"

echo "SQL DDL file created at $SQL_FILE"