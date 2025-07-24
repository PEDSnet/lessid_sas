#!/bin/bash
set -e

input_pattern="$1"
output_dir="$2"
salt="$3"

echo "Input Pattern: $input_pattern"
echo "Output Dir: $output_dir"
echo "Salt: $salt"

for file in $input_pattern; do
    file_basename="$(basename "$file")"
    file_basename_wo_extension=$(echo "$file_basename" | cut -f 1 -d '.')

    echo "Processing file: $file"

    output_path="$output_dir/$file_basename"
    echo "Outputting to: $output_path" 


    sas_log_path="$output_dir/log_$file_basename_wo_extension.log"
    sas_lst_path="$output_dir/lst_$file_basename_wo_extension.lst"
    echo "Logging to $sas_log_path"
    echo "Save lst to $sas_lst_path"
    init_stmt="%let salt = $salt; 
    %let input_path = $file; 
    %let output_path = $output_path;"

    sas -print $sas_lst_path -log $sas_log_path -initstmt "$init_stmt" ./lessid.sas
done

echo "Processed Successfully!"


