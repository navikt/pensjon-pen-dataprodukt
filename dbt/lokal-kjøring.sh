#!/bin/bash
# chmod +x lokal-kjøring.sh

# Setter miljøvariabler for dbt-kjøring lokalt

export ORA_PYTHON_DRIVER_TYPE="thin"
export DBT_DB_SCHEMA="pen_dataprodukt"

# username
printf "Enter db-username (without proxy): "
read DB_USER
DBT_ENV_SECRET_USER="${DB_USER}[${DBT_DB_SCHEMA}]"
export DBT_ENV_SECRET_USER

# password (input hidden)
printf "Enter db-password: "
stty -echo
read DBT_ENV_SECRET_PASS
stty echo
printf "\n"
export DBT_ENV_SECRET_PASS


# Print environment variables
printf "Miljøvariabler satt:\n"
printf "DBT_DB_SCHEMA:          %s\n" "$DBT_DB_SCHEMA"
printf "DBT_ENV_SECRET_USER:    %s\n" "$DBT_ENV_SECRET_USER"
printf "ORA_PYTHON_DRIVER_TYPE: %s\n" "$ORA_PYTHON_DRIVER_TYPE"