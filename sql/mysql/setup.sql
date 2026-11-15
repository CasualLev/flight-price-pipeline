-- Run once as root. Replace the password ONLY when running, don't save it in this file.
CREATE DATABASE IF NOT EXISTS flight_staging;
CREATE USER IF NOT EXISTS 'flight_user'@'%' IDENTIFIED BY 'change_me';
GRANT ALL PRIVILEGES ON flight_staging.* TO 'flight_user'@'%';