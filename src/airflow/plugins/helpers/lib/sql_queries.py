# helpers/lib/sql_queries.py


class SqlQueries:
    ports_table_insert = """
    INSERT INTO ports(
        countryName, portName, unlocode, coordinates, staging_id
    )
    VALUES (
        %(countryName)s,
        %(portName)s,
        %(unlocode)s,
        %(coordinates)s,
        %(staging_id)s
    )
    ON CONFLICT (countryName, portName, unlocode, coordinates)
    DO UPDATE SET
        (countryName, portName, unlocode, coordinates, updated_at)
        = (
            EXCLUDED.countryName,
            EXCLUDED.portName,
            EXCLUDED.unlocode,
            EXCLUDED.coordinates,
            '{updated_at}'
        );
    """

    table_row_count = "SELECT COUNT(*) FROM {table}"

    select_all_query_to_json = """
    SELECT id,
        countryname AS "countryName",
        portname AS "portName",
        coordinates, unlocode
    FROM {table};
    """

    table_updated_count = """
    SELECT COUNT(
        CASE
        WHEN TO_CHAR(updated_at, 'mm-dd-YYYY HH:MM')
            BETWEEN TO_CHAR(DATE '{execution_date}' - INTERVAL '8' MINUTE, 'mm-dd-YYYY HH:MM')
            AND TO_CHAR(DATE '{execution_date}', 'mm-dd-YYYY HH:MM')
        THEN id
        ELSE NULL
        END
    ) AS total_updates 
    FROM {table};
    """
