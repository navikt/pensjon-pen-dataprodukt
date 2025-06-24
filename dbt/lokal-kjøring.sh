#!/bin/bash
# chmod +x lokal-kjøring.sh

# Setter miljøvariabler for dbt-kjøring lokalt

export ORA_PYTHON_DRIVER_TYPE="thin"

# Last inn variabler fra .env
set -a
source .env
set +a

if [[ -z "$DBT_ENV_SECRET_USER" || -z "$DBT_ENV_SECRET_HOST" ]]; then
    echo "Error: DBT_ENV_SECRET_USER og DBT_ENV_SECRET_HOST miljøvariabler ikke satt"
fi

# password (input hidden)
printf "Enter db-password: "
stty -echo
read DBT_ENV_SECRET_PASS
stty echo
printf "\n"
export DBT_ENV_SECRET_PASS


# Print environment variables
printf "Miljøvariabler satt:\n"
printf "DBT_ENV_SECRET_USER:    %s\n" "$DBT_ENV_SECRET_USER"
printf "ORA_PYTHON_DRIVER_TYPE: %s\n" "$ORA_PYTHON_DRIVER_TYPE"