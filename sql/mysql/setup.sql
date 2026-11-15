CREATE DATABASE IF NOT EXISTS flight_staging;
CREATE USER IF NOT EXISTS 'flight_user'@'%' IDENTIFIED BY 'change_me';
CREATE USER IF NOT EXISTS 'flight_user'@'localhost' IDENTIFIED BY 'change_me';
GRANT ALL PRIVILEGES ON flight_staging.* TO 'flight_user'@'%';
GRANT ALL PRIVILEGES ON flight_staging.* TO 'flight_user'@'localhost';