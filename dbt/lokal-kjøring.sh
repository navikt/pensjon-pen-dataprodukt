#!/bin/bash
# chmod +x lokal-kjøring.sh

# Setter miljøvariabler for dbt-kjøring lokalt

source "$(dirname "$0")/.venv/bin/activate"

export ORA_PYTHON_DRIVER_TYPE="thin"

# Last inn variabler fra .env
set -a
source "$(dirname "$0")/.env"
set +a

if [[ -z "$DBT_ENV_USER" || -z "$DBT_ENV_HOST" ]]; then
    echo "Error: DBT_ENV_USER og DBT_ENV_HOST miljøvariabler ikke satt"
fi

#!/bin/bash

echo "Velg DBT_DB_TARGET:"
select db_target in "pen_q2" "pen_q1" "pen_prod_lesekopi" "pen_prod"; do
    if [[ -n "$db_target" ]]; then
        export DBT_DB_TARGET="$db_target"
        echo "DBT_DB_TARGET set to $DBT_DB_TARGET"
        break
    else
        echo "Ugyldig valg. Velg 1, 2, 3, eller 4."
    fi
done

# password (input hidden)
printf "Enter db-password: "
stty -echo
read DBT_ENV_SECRET_PASS
stty echo
printf "\n"
export DBT_ENV_SECRET_PASS


# Print environment variables
printf "Miljøvariabler satt:\n"
printf "DBT_ENV_USER:    %s\n" "$DBT_ENV_USER"
printf "ORA_PYTHON_DRIVER_TYPE: %s\n" "$ORA_PYTHON_DRIVER_TYPE"
