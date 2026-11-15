-- Run once as the postgres superuser. Run each statement separately.
CREATE USER flight_user WITH PASSWORD 'change_me';
CREATE DATABASE flight_analytics OWNER flight_user;