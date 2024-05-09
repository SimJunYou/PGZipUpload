CREATE UNLOGGED TABLE ais_staging (
    mmsi TEXT, 
    imo TEXT,
    vessel_name TEXT,
    callsign TEXT,
    vessel_type TEXT,
    vessel_type_code TEXT,
    vessel_type_cargo TEXT,
    vessel_class TEXT,
    length FLOAT,
    width FLOAT,
    flag_country TEXT,
    flag_code TEXT,
    destination TEXT,
    eta TEXT, -- to cast to datetime
    draught FLOAT,
    position TEXT, -- to change into PostGIS points
    longitude FLOAT,
    latitude FLOAT,
    sog FLOAT,
    cog FLOAT,
    rot FLOAT,
    heading FLOAT, 
    nav_status TEXT,
    nav_status_code TEXT,
    source TEXT,
    ts_pos_utc TEXT, -- to cast to datetime
    ts_static_utc TEXT, -- to cast to datetime
    dt_pos_utc TEXT, -- to cast to datetime
    dt_static_utc TEXT, -- to cast to datetime
    vessel_type_main TEXT,
    vessel_type_sub TEXT,
    message_type TEXT,
    eeid TEXT,
    dtg TEXT -- to cast to datetime
); 
