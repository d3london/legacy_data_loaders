#!/bin/bash

# Displays CSVs files in directory
# User may select a file to copy into mounted docker postgres
# Have hard-coded source and destination for now...
#
# Usage: select_copy_csv_to_docker.sh

# Directory containing the raw CSV files
SOURCE_DIR="/srv/shared/"

# Mounted directory for postgres docker container
DEST_DIR="/var/lib/postgresql/data/sources/"

# Docker container
CONTAINER_NAME="omop_postgres"

# Check if the dir exists already, create if doesn't
docker exec $CONTAINER_NAME bash -c "[ -d $DEST_DIR ] || mkdir -p $DEST_DIR"

# List CSV files, let user choose one to copy
echo "Available CSV files:"
files=(${SOURCE_DIR}*.csv)
index=1
for file in "${files[@]}"; do
    echo "$index) $(basename "$file")"
    let index++
done

read -p "Enter number of the file you want to copy: " choice
selected_file="${files[$choice-1]}"
filename=$(basename "$selected_file")

# Check if the file already exists
if docker exec $CONTAINER_NAME [ -f ${DEST_DIR}${filename} ]; then
    # File exists, ask the user if they want to overwrite
    echo "$filename already exists in the container."
    read -p "Do you want to overwrite it? (y/n): " confirm
    if [[ "$confirm" == "y" ]]; then
        echo "Overwriting $filename in the container..."
        docker cp "$selected_file" "$CONTAINER_NAME:$DEST_DIR"
    else
        echo "Skipping $filename..."
    fi
else
    # File does not exist in the container, copy it over
    echo "Copying $filename to the container..."
    docker cp "$selected_file" "$CONTAINER_NAME:$DEST_DIR"
fi

echo "Operation complete."