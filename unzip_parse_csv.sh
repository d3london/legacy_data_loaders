#!/bin/bash

#########################################################################
# AI Centre & London SDE
# Process compressed CSV files prior to Postgres load
#
# Usage: $ ./unzip_parse_csv.sh
# - Unzips compressed CSV in /srv/shared/ as temporary file
# - Uses AWK to detect and escape fields with carriage returns
# - Catches non-UTF-8 characters and passes to error file
# - Catches rows with too many columns (e.g. delimiter in field) and passes to error file
# - Cleaned files and error files are output into /srv/shared/sources
#
# Reqirements: 
# - AWK
# - iconv
# - Raw CSVs should be pipe delimited, UTF-8 encoded, Use LF for newline
#########################################################################

# Cause script fail to exit on errors, add -x for debugging
set -euo pipefail

# Set directories
SOURCE_DIR="/srv/shared/" # compressed CSV files should be staged here in .zip
OUTPUT_DIR="/srv/shared/sources/" # unzipped and parsed CSV files are pushed here

# If there are no ZIP files in the source directory then exit
if [ -z "$(ls "${SOURCE_DIR}"*.zip 2>/dev/null)" ]; then
    echo "Error: No ZIP files found in ${SOURCE_DIR}"
    exit 1
fi

# List ZIP files, let user choose one
echo "Available ZIP files:"
files=(${SOURCE_DIR}*.zip) # create array to store list of files
index=1
for file in "${files[@]}"; do
    echo "$index) $(basename "$file")"
    ((index++))
done

read -p "Enter number of the file you want to process: " choice
selected_file="${files[$choice-1]}" # array index starts at 0
filename=$(basename "$selected_file")

# Unzip seleted file as temp
if [[ "$filename" == *.zip ]]; then
    echo "Unzipping file..."
    temp_dir=$(mktemp -d)
    unzip -q "$selected_file" -d "$temp_dir"
    csv_files=("$temp_dir"/*.csv)
    if [ ${#csv_files[@]} -ne 1 ]; then
        echo "Error: ZIP must contain exactly one CSV file."
        rm -rf "$temp_dir"
        exit 1
    fi
    selected_file="${csv_files[0]}"
    filename=$(basename "$selected_file")
fi

# Autodetect number of columns (MUST BE PIPE DELIMITED)
DETECTED_COLUMNS=$(head -n 1 "$selected_file" | awk -F'|' '{print NF}')
echo "Detected number of columns: $DETECTED_COLUMNS"
read -p "Press ENTER if this number is correct, or input a different number to override: " USER_COLUMNS

NUM_FIELDS=${USER_COLUMNS:-$DETECTED_COLUMNS}

# Generate output file name
OUTPUT_FILE="${OUTPUT_DIR}source__${filename}"
TEMP_FILE=$(mktemp)
ERROR_FILE="${OUTPUT_DIR}error_encoding_${filename}"
ERROR_TABLEWIDTH_FILE="${OUTPUT_DIR}error_tablewidth_${filename}"

# Count and print number of rows in original file (for comparison)
echo "Counting number of rows..."
original_count=$(wc -l < "$selected_file")
echo "Original row count: $original_count"

# Processing with AWK
# Counts number of fields per line, if less than NUM_FIELDS then stores as incopmlete
# For each field, escapes those with carriage returns
# Reconstructs until correct number of fields and passes to next line 

echo "Processing with AWK..."
awk -F'|' -v OFS='|' -v num_fields="$NUM_FIELDS" '
{
    if (holding != "") {
        record = holding $0;
    } else {
        record = $0;
    }
    
    field_count = gsub(/\|/, "|", record) + 1;
    
    if (field_count < num_fields) {
        holding = record;
    } else {
        split(record, fields, FS);
        for (i = 1; i <= length(fields); i++) {
            if (fields[i] ~ /\r/) {
                fields[i] = "\"" fields[i] "\"";
            }
        }
        record = fields[1];
        for (i = 2; i <= length(fields); i++) {
            record = record OFS fields[i];
        }
        print record;
        holding = "";
    }
}' "$selected_file" > "$TEMP_FILE"

# Passes UTF-8 to UTF-8 skipping non-conforming characters
# (All CSVs should be originally exported with UTF-8 encoding anyway)   
# Capture invalid characters in the error file
echo "Parsing UTF-8 encoding..."
iconv -f utf-8 -t utf-8 -c "$TEMP_FILE" > "$OUTPUT_FILE" 2> >(sed 's/iconv: //' > "$ERROR_FILE")

# Display invalid characters if any were found
if [ -s "$ERROR_FILE" ]; then
    echo "Warning: Non-conforming UTF-8 characters were found. See details in: $ERROR_FILE"
else
    echo "No non-conforming UTF-8 characters were found."
    rm "$ERROR_FILE"  # Remove the error file if it's empty
fi

# Check field counts in the processed CSV file
# If too many fields, removes the row and passes to error file 
echo "Checking field counts in processed file..."
awk -F'|' -v num_fields="$NUM_FIELDS" '
BEGIN {mismatch_count=0}
NR==1 {header_count=NF; if (header_count != num_fields) print "Warning: Header has " header_count " fields, expected " num_fields; next} 
NF!=num_fields {
    print "Mismatch on line " NR ": expected " num_fields " fields, found " NF
    mismatch_count++
}
END {
    if (mismatch_count == 0) {
        print "All rows have the correct number of fields."
    } else {
        print "Total mismatches found: " mismatch_count
    }
}
' "$OUTPUT_FILE" | tee "$ERROR_TABLEWIDTH_FILE"

# Remove temporary files
rm "$TEMP_FILE"
[ -d "$temp_dir" ] && rm -rf "$temp_dir"

# Count final rows
echo "Counting final rows..."
final_count=$(wc -l < "$OUTPUT_FILE")
echo "Final row count: $final_count"

echo "Processing complete. Output file: $OUTPUT_FILE"