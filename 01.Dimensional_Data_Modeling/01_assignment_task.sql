CREATE TYPE films AS (
		films TEXT, 
		votes INTEGER,
		rating REAL,		
        filmid INTEGER
)

CREATE TYPE quality_class AS 
    ENUM('star','good','average','bad')

CREATE TABLE actors (
    actor_id INTEGER,
    actor_name TEXT,
    films films[],
    quality_class quality_class,
    is_active BOOLEAN,
    year_film INTEGER,    
    PRIMARY KEY(actor_id)
)