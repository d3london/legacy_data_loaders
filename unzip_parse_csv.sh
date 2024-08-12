#!/bin/bash

# AI Centre & London SDE
# Process compressed CSV files prior to Postgres load

# Usage: $ ./unzip_parse_csv.sh
# - Unzips as temporary file
# - Fixes fields with carriage returns, formatting multi-line records, and filtering non-UTF-8 characters.
# - Saves into /srv/shared/sources/ 
# Individual raw CSV files compressed as .zip in /srv/shared
# Cleaned files output as source__{filename}.csv into /srv/shared/sources   
# Raw CSVs should be:
# - Pipe delimited
# - UTF-8 encoded
# - Use LF for newline

SOURCE_DIR="/srv/shared/" # compressed CSV files should be staged here in .zip
OUTPUT_DIR="/srv/shared/sources/" # unzipped and parsed CSV files are pushed here

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
TEMP_FILE="/tmp/temp__${filename}"

# Count and print number of rows in original file (for comparison)
original_count=$(wc -l < "$selected_file")
echo "Original row count: $original_count"

# Processing with AWK
# Counts number of fields per line, if less than NUM_FIELDS then stores as incopmlete
# For each field, escapes those with carriage returns
# Reconstructs until correct number of fields and passes to next line 

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
# Assumes that all CSVs are originally exported with UTF-8 encoding anyway   
iconv -f utf-8 -t utf-8 -c "$TEMP_FILE" > "$OUTPUT_FILE"

# Remove temporary files
rm "$TEMP_FILE"
[ -d "$temp_dir" ] && rm -rf "$temp_dir"

# Count final rows
final_count=$(wc -l < "$OUTPUT_FILE")
echo "Final row count: $final_count"

echo "Processing complete. Output file: $OUTPUT_FILE"