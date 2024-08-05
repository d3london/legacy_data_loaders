#!/bin/bash

# Uses AWK to parse csv and fix fields with carriage return (CR)
# and correctly format multi-line records
# Filters out non-UTF-8 characters using iconv
#
# Usage: brute_fix_csv.sh <source_file> <number_of_columns>

# Check for two args
if [ $# -ne 2 ]; then
    echo "Usage: $0 <source_file> <number_of_columns>"
    exit 1
fi

SOURCE_FILE=$1
NUM_FIELDS=$2

# Generate output file name
OUTPUT_FILE="fixed__${SOURCE_FILE}"
TEMP_FILE="temp__${SOURCE_FILE}"

# Count and print original rows
original_count=$(wc -l < "$SOURCE_FILE")
echo "Original row count: $original_count"

# Processing with awk
# Assumes pipe delim 
awk -F'|' -v OFS='|' -v num_fields="$NUM_FIELDS" '{
    
    # Initiate holding buffer, store current line
    if (holding != "") {
        record = holding $0;
    } else {
        record = $0;
    }
    
    # Count the number of fields via delim
    field_count = gsub(/\|/, "|", record) + 1;
    
    # If the record is incomplete append next line to buffer
    if (field_count < num_fields) {
        holding = record;
    } else {
        # Check and quote fields with CR 
        # (prevent incorrect line break interpretation)
        split(record, fields, FS);
        for (i = 1; i <= length(fields); i++) {
            if (fields[i] ~ /\r/) {
                fields[i] = "\"" fields[i] "\"";  # Enclose in quotes if CR
            }
        }
        # Reconstruct from array with pipe
        record = fields[1];
        for (i = 2; i <= length(fields); i++) {
            record = record OFS fields[i];
        }
        
        # Print corrected record
        print record;
        
        # Clear buffer
        holding = "";
    }
}' "$SOURCE_FILE" > "$TEMP_FILE"

# Force convert encoding to UTF-8, skipping non-conforming characters
iconv -f utf-8 -t utf-8 -c "$TEMP_FILE" > "$OUTPUT_FILE"

# Remove the temporary file
rm "$TEMP_FILE"

# Count final rows
final_count=$(wc -l < "$OUTPUT_FILE")
echo "Final row count: $final_count"

echo "Processing complete. Output file: $OUTPUT_FILE"