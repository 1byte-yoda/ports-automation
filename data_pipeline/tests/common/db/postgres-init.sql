CREATE TABLE IF NOT EXISTS "ports" (
    id SERIAL PRIMARY KEY,
	staging_id VARCHAR(50),
	portName VARCHAR(255) NOT NULL,
	unlocode VARCHAR (50) NOT NULL,
	countryName VARCHAR (255) NOT NULL,
    coordinates VARCHAR (255),
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX ON "ports"(
	countryName,
	portName,
	unlocode,
	coordinates
);