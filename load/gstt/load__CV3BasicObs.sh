#!/bin/bash

# Loads source file into OMOP postgres 
# Usage "load__CV3BasicObs.sh {csv_file_name} {table_name}"
# The source file path and the database.schema are hard-coded
# Requires the uuid-ossp extension to be installed

# Arguments: CSV file name and table name
CSV_FILE="$1"
TABLE_NAME="$2"

# Docker container name
CONTAINER_NAME="omop_postgres"
DATABASE_NAME="omop_dev"

# PostgreSQL credentials from environment variables
PG_USER="$PG_USER"
PG_PASSWORD="$PG_PASSWORD"

# Path to CSV file in the Docker container
CSV_PATH="/var/lib/postgresql/data/sources/$CSV_FILE"

# Path to psql inside the Docker container
PSQL="docker exec -i $CONTAINER_NAME psql -U $PG_USER -d $DATABASE_NAME"

# Test connection to database and list tables in source
echo "Testing database connection:"
$PSQL -c "\dn"
$PSQL -c "\dt source.*"

# If table exists, ask to drop
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

# Create table from DDL
echo "Creating the table source.$TABLE_NAME:"
$PSQL <<EOF
CREATE TABLE source.$TABLE_NAME (
    source_uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    "GUID" DECIMAL,
    "ClientVisitGUID" DECIMAL,
    "ClientGUID" DECIMAL,
    "OrderGUID" DECIMAL,
    "MasterGUID" DECIMAL,
    "ChartGUID" VARCHAR,
    "ResultItemGUID" VARCHAR,
    "Status" VARCHAR,
    "Active" BOOLEAN,
    "AbnormalityCode" VARCHAR,
    "ItemName" VARCHAR,
    "ClusterID" DECIMAL,
    "Value" VARCHAR,
    "ReferenceUpperLimit" VARCHAR,
    "ReferenceLowerLimit" VARCHAR,
    "UnitOfMeasure" VARCHAR,
    "TypeCode" VARCHAR,
    "IsHistory" BOOLEAN,
    "IsTextual" BOOLEAN,
    "HasHistory" BOOLEAN,
    "HasMediaLink" BOOLEAN,
    "AckStatusNum" DECIMAL,
    "Entered" TIMESTAMP WITHOUT TIME ZONE,
    "ArrivalDtm" TIMESTAMP WITHOUT TIME ZONE,
    "TouchedWhen" TIMESTAMP WITHOUT TIME ZONE,
    "CreatedWhen" TIMESTAMP WITHOUT TIME ZONE,
    source_table_provenance VARCHAR DEFAULT '$CSV_FILE'
);
EOF
echo "Table created."

# Load data from csv
read -p "Do you want to proceed to load data into the table? (y/n): " confirm_load
if [ "$confirm_load" = "y" ]; then
    $PSQL <<EOF
\COPY source.$TABLE_NAME("GUID", "ClientVisitGUID", "ClientGUID", "OrderGUID", "MasterGUID", "ChartGUID", "ResultItemGUID", "Status", "Active", "AbnormalityCode", "ItemName", "ClusterID", "Value", "ReferenceUpperLimit", "ReferenceLowerLimit", "UnitOfMeasure", "TypeCode", "IsHistory", "IsTextual", "HasHistory", "HasMediaLink", "AckStatusNum", "Entered", "ArrivalDtm", "TouchedWhen", "CreatedWhen") FROM '$CSV_PATH' WITH (FORMAT csv, HEADER true, DELIMITER '|', NULL '');
EOF
    echo "Data loaded successfully."
else
    echo "Data load cancelled."
fi
