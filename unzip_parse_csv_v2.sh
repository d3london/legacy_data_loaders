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

# HArd-coding directories to preserve consistency
SOURCE_DIR="/srv/shared/" # compressed CSV files should be staged here in .zip
OUTPUT_DIR="/srv/shared/sources/" # unzipped and parsed CSV files are pushed here

# Function: check if ZIP files exist
check_zip_files() {
    if [ -z "$(ls "${SOURCE_DIR}"*.zip 2>/dev/null)" ]; then
        echo "Error: No ZIP files found in ${SOURCE_DIR}" >&2
        exit 1
    fi
}

# Function: list and select ZIP file
select_zip_file() {
    echo "Available ZIP files:"
    mapfile -t files < <(find "${SOURCE_DIR}" -name "*.zip")
    for i in "${!files[@]}"; do
        echo "$((i+1))) $(basename "${files[i]}")"
    done

    read -p "Enter number of the file you want to process: " choice
    if ! [[ "$choice" =~ ^[0-9]+$ ]] || [ "$choice" -lt 1 ] || [ "$choice" -gt "${#files[@]}" ]; then
        echo "Invalid choice. Exiting." >&2
        exit 1
    fi
    echo "${files[$choice-1]}"
}

# Function: unzip and validate presence of single CSV file
unzip_and_validate() {
    local zip_file="$1"
    local temp_dir="$2"

    echo "Unzipping file..."
    unzip -q "$zip_file" -d "$temp_dir"
    local csv_files=("$temp_dir"/*.csv)
    if [ ${#csv_files[@]} -ne 1 ]; then
        echo "Error: ZIP must contain exactly one CSV file." >&2
        exit 1
    fi
    echo "${csv_files[0]}"
}

# Function: detect and confirm column count for subsequent cleaning
detect_columns() {
    local csv_file="$1"
    local detected_columns
    detected_columns=$(head -n 1 "$csv_file" | awk -F'|' '{print NF}')
    echo "Detected number of columns: $detected_columns"
    read -p "Press ENTER if this number is correct, or input a different number to override: " user_columns
    echo "${user_columns:-$detected_columns}"
}

# Function: process CSV with AWK (count fields, escape newlines, and check field counts)
process_and_check_csv() {
    local input_file="$1"
    local output_file="$2"
    local error_file="$3"
    local num_fields="$4"

    awk -F'|' -v OFS='|' -v num_fields="$num_fields" '
    BEGIN { 
        holding = ""; 
        mismatch_count = 0;
    }
    {
        if (holding != "") {
            record = holding $0
        } else {
            record = $0
        }
        
        field_count = gsub(/\|/, "|", record) + 1
        
        if (field_count < num_fields) {
            holding = record
        } else {
            if (field_count != num_fields) {
                print "Mismatch on line " NR ": expected " num_fields " fields, found " field_count > "/dev/stderr"
                mismatch_count++
            }
            
            split(record, fields, FS)
            for (i = 1; i <= length(fields); i++) {
                if (fields[i] ~ /\r/) {
                    gsub(/\"/, "\"\"", fields[i])
                    fields[i] = "\"" fields[i] "\""
                }
            }
            print join(fields, OFS)
            holding = ""
        }
    }
    END {
        if (holding != "") {
            print "Warning: Incomplete record at end of file: " holding > "/dev/stderr"
        }
        if (mismatch_count == 0) {
            print "All rows have the correct number of fields." > "/dev/stderr"
        } else {
            print "Total mismatches found: " mismatch_count > "/dev/stderr"
        }
    }
    function join(array, sep,    result, i) {
        if (length(array) == 0) return ""
        result = array[1]
        for (i = 2; i <= length(array); i++)
            result = result sep array[i]
        return result
    }
    ' "$input_file" > "$output_file" 2> "$error_file"
}

# Function: handle UTF-8 encoding
handle_encoding() {
    local input_file="$1"
    local output_file="$2"
    local error_file="$3"

    iconv -f utf-8 -t utf-8 -c "$input_file" > "$output_file" 2> >(sed 's/iconv: //' > "$error_file")

    if [ -s "$error_file" ]; then
        echo "Warning: Non-conforming UTF-8 characters were found. See details in: $error_file"
    else
        echo "No non-conforming UTF-8 characters were found."
        rm "$error_file"
    fi
}

# Main
main() {
    check_zip_files

    local selected_file
    selected_file=$(select_zip_file)

    local filename
    filename=$(basename "$selected_file")

    local temp_dir
    temp_dir=$(mktemp -d)
    trap 'rm -rf "$temp_dir"' EXIT

    local csv_file
    csv_file=$(unzip_and_validate "$selected_file" "$temp_dir")
    filename=$(basename "$csv_file")

    local num_fields
    num_fields=$(detect_columns "$csv_file")

    local output_file="${OUTPUT_DIR}source__${filename}" # parsed final CSV
    local error_rowwidth_file="${OUTPUT_DIR}error_rowwidth_${filename}" # error output for rows that exceed table width
    local error_encoding_file="${OUTPUT_DIR}error_encoding_${filename}" # error output for characters that are not UTF-8 

    local original_count
    original_count=$(wc -l < "$csv_file")
    echo "Original row count: $original_count"
    
    echo "Processing and checking CSV file using AWK..."
    process_and_check_csv "$csv_file" "$output_file" "$error_rowwidth_file" "$num_fields"

    echo "Checking encoding..."
    handle_encoding "$output_file" "${output_file}.tmp" "$error_encoding_file"
    mv "${output_file}.tmp" "$output_file"

    local final_count
    final_count=$(wc -l < "$output_file")
    echo "Final row count: $final_count"

    echo "Processing complete. Output file: $output_file"
    echo "Row width error file: $error_rowwidth_file"
    echo "Encoding error file: $error_encoding_file"
}

# Execute
main
