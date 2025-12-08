#!/bin/bash
# chmod +x lokal-kjøring.sh

# Last inn brukernavn og host fra .env (kjører export av DBT_ENV_USER og DBT_ENV_HOST)
set -a
source "$(dirname "$0")/.env"
set +a

if [[ -z "$DBT_ENV_USER" || -z "$DBT_ENV_HOST" ]]; then
    echo "Error: DBT_ENV_USER og DBT_ENV_HOST miljøvariabler ikke satt"
fi

# DBT_DB_TARGET velger target i profiles.yml
echo "Velg DBT_DB_TARGET:"
select db_target in "pen_q2" "pen_prod_lesekopi" "pen_prod"; do
    if [[ -n "$db_target" ]]; then
        export DBT_DB_TARGET="$db_target"
        echo "DBT_DB_TARGET set to $DBT_DB_TARGET"
        break
    else
        echo "Ugyldig valg. Velg 1, 2, eller 3."
    fi
done

# password med skjult input
printf "Enter db-password: "
stty -echo
read DBT_ENV_SECRET_PASS
stty echo
printf "\n"
export DBT_ENV_SECRET_PASS

export ORA_PYTHON_DRIVER_TYPE="thin"

# Printer ut bruker
printf "Miljøvariabler satt:\n"
printf "DBT_ENV_USER:    %s\n" "$DBT_ENV_USER"
