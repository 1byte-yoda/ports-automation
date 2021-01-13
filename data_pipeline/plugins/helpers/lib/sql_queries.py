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

    ports_row_count = """
    SELECT COUNT(*) AS "count"
    FROM ports
    WHERE (
        countryName IS NOT NULL
        OR portName IS NOT NULL
        OR unlocode IS NOT NULL
        OR coordinates IS NOT NULL
        OR staging_id IS NOT NULL
    );
    """

    select_all_query_to_json = """
    SELECT id,
        countryname AS "countryName",
        portname AS "portName",
        coordinates, unlocode
    FROM {table};
    """

    ports_updated_count = """
    SELECT COUNT(
        CASE
        WHEN (
            TO_CHAR(updated_at, 'mm-dd-YYYY') = TO_CHAR(NOW(), 'mm-dd-YYYY')
        ) OR (
            TO_CHAR(created_at, 'mm-dd-YYYY') = TO_CHAR(NOW(), 'mm-dd-YYYY')
        )
        THEN id
        ELSE NULL
        END
    ) AS "count"
    FROM ports
    WHERE (
        countryName IS NOT NULL
        OR portName IS NOT NULL
        OR unlocode IS NOT NULL
        OR coordinates IS NOT NULL
        OR staging_id IS NOT NULL
    );
    """
