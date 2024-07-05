#!/bin/bash

set -xe

# Define an array of table names
tables=()

# Define an array of IDs to filter the query
ids=()

START_DATE="2024-01-30"
END_DATE="2024-02-03"

# Function to format date
format_date() {
    date -I -d "$1"
}

# Define the namespace and pod name
POD_NAME="clickhouse-0"

# Loop through each table name
for table in "${tables[@]}"; do
  # Loop through each ID
  for id in "${ids[@]}"; do
    current_date=$(format_date "$START_DATE")
    while [ "$current_date" != $(format_date "$END_DATE") ]; do
      # Define the output file name
      output_file="${table}_${id}_${current_date}.csv"
      download_file="${table}/${id}_${current_date}.csv"

      query="SELECT * FROM testenv.${table} WHERE id = '${id}' AND toDate(date) = toDate('$current_date') FORMAT CSVWithNames"

      echo "Executing query for table '${table}' with id '${id}': ${query}"

      # Execute the ClickHouse query in the pod and save the output to a temporary file in the pod
      kubectl exec $POD_NAME -- sh -c "clickhouse-client --query=\"${query}\" > /tmp/${output_file}"

      # Check if the command executed successfully
      if [ $? -eq 0 ]; then
        # Create directory if it doesn't exist
        mkdir -p ${table}

        # Copy the temporary file from the pod to the local machine
        kubectl cp --retries=100 $POD_NAME:/tmp/${output_file} ./${download_file}

        # Print a success message if the data was written successfully
        echo "Data from table '${table}' with id '${id}' written to '${download_file}'"

        # Remove the temporary file from the pod
        kubectl exec $POD_NAME -- rm /tmp/${output_file}
      else
        # Print an error message if the data write failed
        echo "Failed to write data from table '${table}' with id '${id}'"
      fi
      # Increment date
      current_date=$(date -I -d "$current_date + 1 day")
    done
  done
done
