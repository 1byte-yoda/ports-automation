#!/usr/bin/env bash

TRY_LOOP="20"

: "${POSTGRES_HOST:="postgresqldb"}"
: "${POSTGRES_PORT:="5432"}"
: "${POSTGRES_DB:="airflow"}"

# Defaults and back-compat
: "${AIRFLOW_HOME:="/usr/local/airflow"}"
: "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"
: "${AIRFLOW__CORE__EXECUTOR:=${EXECUTOR:-Sequential}Executor}"
: "${AIRFLOW__CORE__SMTP_PASSWORD:=${SMTP_PASSWORD}}"
: "${AIRFLOW__CORE__SMTP_USER:=${SMTP_USER}}"
: "${AIRFLOW_CONN_MONGO_STAGING:=${MONGO_DB_URI}}"
: "${AIRFLOW_CONN_POSTGRES_MASTER:=${POSTGRESQL_URI}}"
: "${AIRFLOW_VAR_SLACK_API_KEY:=${SLACK_API_KEY}}"

echo "export AIRFLOW_HOME=${AIRFLOW_HOME}" >> ~/.bashrc
echo "export AIRFLOW__CELERY__BROKER_URL=${AIRFLOW__CELERY__BROKER_URL}" >> ~/.bashrc
echo "export AIRFLOW__CELERY__RESULT_BACKEND=${AIRFLOW__CELERY__RESULT_BACKEND}" >> ~/.bashrc
echo "export AIRFLOW__CORE__EXECUTOR=${AIRFLOW__CORE__EXECUTOR}" >> ~/.bashrc
echo "export AIRFLOW__CORE__FERNET_KEY=${AIRFLOW__CORE__FERNET_KEY}" >> ~/.bashrc
echo "export AIRFLOW__CORE__SMTP_PASSWORD=${AIRFLOW__CORE__SMTP_PASSWORD}" >> ~/.bashrc
echo "export AIRFLOW__CORE__SMTP_USER=${AIRFLOW__CORE__SMTP_USER}" >> ~/.bashrc
echo "export AIRFLOW_CONN_MONGO_STAGING=${AIRFLOW_CONN_MONGO_STAGING}" >> ~/.bashrc
echo "export AIRFLOW_CONN_POSTGRES_MASTER=${AIRFLOW_CONN_POSTGRES_MASTER}" >> ~/.bashrc
echo "export AIRFLOW_VAR_SLACK_API_KEY=${SLACK_API_KEY}" >> ~/.bashrc

export \
  AIRFLOW_HOME \
  AIRFLOW__CELERY__BROKER_URL \
  AIRFLOW__CELERY__RESULT_BACKEND \
  AIRFLOW__CORE__EXECUTOR \
  AIRFLOW__CORE__FERNET_KEY \
  AIRFLOW__CORE__LOAD_EXAMPLES \
  AIRFLOW__CORE__SQL_ALCHEMY_CONN \
  AIRFLOW__SMTP__SMTP_PASSWORD \
  AIRFLOW__SMTP__SMTP_USER \
  AIRFLOW_CONN_MONGO_STAGING \
  AIRFLOW_CONN_POSTGRES_MASTER \
  AIRFLOW_VAR_SLACK_API_KEY

# Load DAGs examples (default: Yes)
if [[ -z "$AIRFLOW__CORE__LOAD_EXAMPLES" && "${LOAD_EX:=n}" == n ]]
then
  AIRFLOW__CORE__LOAD_EXAMPLES=False
  echo "export AIRFLOW__CORE__LOAD_EXAMPLES=${AIRFLOW__CORE__LOAD_EXAMPLES}" >> ~/.bashrc
fi

# Install custom python package if requirements.txt is present
# if [ -e "/requirements.txt" ]; then
#     $(command -v pip) install --user -r /requirements.txt
# fi

wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [ $j -ge $TRY_LOOP ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name... $j/$TRY_LOOP"
    sleep 5
  done
}

AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
wait_for_port "Postgres" "$POSTGRES_HOST" "$POSTGRES_PORT"
echo "export AIRFLOW__CORE__SQL_ALCHEMY_CONN=${AIRFLOW__CORE__SQL_ALCHEMY_CONN}" >> ~/.bashrc
echo "export AIRFLOW__CELERY__RESULT_BACKEND=${AIRFLOW__CELERY__RESULT_BACKEND}" >> ~/.bashrc

case "$1" in
  webserver)
    airflow db init
    if [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ]; then
      # With the "Local" and "Sequential" executors it should all run in one container.
      airflow scheduler &
    fi
    exec airflow webserver
    exec airflow variables -s slack_api_key "$AIRFLOW_VAR_SLACK_API_KEY"
    exec airflow connections add 'mongo_staging' "$AIRFLOW_CONN_MONGO_STAGING"
    exec airflow connections add 'postgres_master' "$AIRFLOW_CONN_POSTGRES_MASTER"
    ;;
  worker|scheduler)
    # Give the webserver time to run initdb.
    sleep 10
    exec airflow "$@"
    ;;
  flower)
    sleep 10
    exec airflow "$@"
    ;;
  version)
    exec airflow "$@"
    ;;
  *)
    # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
    exec "$@"
    ;;
esac
