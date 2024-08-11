#!/bin/bash

# From a selected CSV file in a specific directory, generates a postgres DDL for table creation
# Usage: 
# ./generate_ddl.sh

# Directory containing CSV files
CSV_DIR="/srv/shared/sources/"

# Check if csvkit is installed, install if not
if ! command -v csvsql &> /dev/null
then
    echo "csvsql could not be found, installing csvkit..."
    sudo apt update
    sudo apt install python3-pip -y
    sudo pip3 install csvkit
fi

# List all CSV files in the directory
echo "CSV files available in $CSV_DIR:"
CSV_FILES=($(ls "$CSV_DIR"*.csv))
for i in "${!CSV_FILES[@]}"; do
    echo "$((i+1)). ${CSV_FILES[$i]##*/}"
done

# Prompt user to select a file
echo "Enter the number of the CSV file you want to process:"
read -r selection

# Validate user input
if ! [[ "$selection" =~ ^[0-9]+$ ]] || [ "$selection" -lt 1 ] || [ "$selection" -gt "${#CSV_FILES[@]}" ]; then
    echo "Invalid selection. Exiting."
    exit 1
fi

# Get the selected CSV file
CSV_FILE="${CSV_FILES[$((selection-1))]}"
CSV_FILENAME=$(basename "$CSV_FILE")

# Set the output SQL file name
SCRIPT_DIR=$(dirname "$0")
SQL_FILE="$SCRIPT_DIR/ddl_${CSV_FILENAME%.csv}.sql"

# Generate SQL DDL from CSV
echo "Generating SQL DDL from $CSV_FILENAME..."
csvsql --dialect postgresql --snifflimit 1000 --delimiter '|' "$CSV_FILE" > "$SQL_FILE"

echo "SQL DDL file created at $SQL_FILE"