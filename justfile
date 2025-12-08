default:
    @just --list --unsorted

# Installerer nødvendige pakker for andre kommandoer
bootstrap:
    @printf "Sørger for at nødvendige pakker er installert\n"
    @uv --version > /dev/null 2>&1 || brew install uv
    @nais --version > /dev/null 2>&1 || brew install nais || brew tap nais/tap

# Setter opp miljøet
install: bootstrap
    @printf "Setter opp miljøet\n"
    @uv sync --dev

# Oppgrader dependencies og setter opp miljøet på nytt
update: bootstrap
    @printf "Oppgraderer dependencies\n"
    @uv sync --upgrade --dev

# Kjører dbt-prosjektet
run:
    @printf "Kjører dbt, må skje fra dbt-mappa\n"
    @uv run dbt run
