INIT_FILE=.airflowinitialized
DEFAULT_DB=airflow.db
if [ ! -f $INIT_FILE ]; then
    # Setup Airflow's variables
    : "${AIRFLOW_VAR_SLACK_API_KEY:=${SLACK_API_KEY}}"
    echo "export AIRFLOW_VAR_SLACK_API_KEY=${SLACK_API_KEY}" >> ~/.bashrc
    export AIRFLOW_VAR_SLACK_API_KEY

    # Install netcat
    apt update && apt install -y netcat

    # Initialize db to force creation of airflow.cfg file
    airflow db init
    if [ -f $DEFAULT_DB ]; then
        rm airflow.db
    fi
    sleep 5

    # Wait until the DB is ready
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

    # Setup Airflow's config [with DB]
    python config_setup.py

    wait_for_port "Postgres" "$POSTGRES_HOST" "$POSTGRES_PORT"
fi

# Run the Airflow webserver and scheduler

if [ ! -f $INIT_FILE ]; then
    echo "INITIALIZING DATABASE..."
    sleep 5
    airflow db init

    echo "CREATING DATABASE RECORDS..."

    echo "export AIRFLOW_VAR_SLACK_API_KEY=${SLACK_API_KEY}" >> ~/.bashrc
    export AIRFLOW_VAR_SLACK_API_KEY &&
    sleep 10

    python init_connections.py &&
    airflow users create -r $AIRFLOW_USER_ROLE -u $AIRFLOW_USERNAME \
    -e $AIRFLOW_USER_EMAIL -f $AIRFLOW_USER_FIRSTNAME \
    -l $AIRFLOW_USER_LASTNAME -p $AIRFLOW_PASSWORD

    # This configuration is done only the first time
    touch $INIT_FILE
fi

echo "STARTING SCHEDULER..."
sleep 5
airflow scheduler &

echo "STARTING WEBSERVER..."
sleep 5
airflow webserver &
wait 