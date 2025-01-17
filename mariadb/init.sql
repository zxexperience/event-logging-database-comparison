CREATE TABLE IF NOT EXISTS Event (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    message VARCHAR(255) NOT NULL,
    severity_ID INT NOT NULL,
    event_type_ID INT NOT NULL,
    source_ID INT NOT NULL
);

CREATE TABLE IF NOT EXISTS Severity (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(63) NOT NULL,
    description VARCHAR(255)
);

INSERT INTO Severity (name, description) VALUES
('Info', 'Informational message'),
('Warning', 'Warning condition'),
('Error', 'Error condition'),
('Critical', 'Critical condition');

CREATE TABLE IF NOT EXISTS Event_type (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(63) NOT NULL,
    description VARCHAR(255)
);

INSERT INTO Event_type (name, description) VALUES
('SYSTEM_STATUS', 'System status update'),
('SECURITY_ALERT', 'Security-related event'),
('PERFORMANCE','Performance metric event'),
('USER_ACTION', 'User-initiated action');


CREATE TABLE IF NOT EXISTS Source (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(63) NOT NULL,
    ip_address VARCHAR(45) NOT NULL,
    location_id INT NOT NULL
);

INSERT INTO Source (name, ip_address, location_id) VALUES
('web-server-01', '192.168.1.100', 1),
('web-server-02', '192.168.1.200', 2),
('cache-01', '192.168.2.100', 3),
('lb-01', '192.168.3.100', 4);


CREATE TABLE IF NOT EXISTS Location (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(63) NOT NULL,
    country VARCHAR(63) NOT NULL,
    city VARCHAR(63) NOT NULL
);

INSERT INTO Location (name, country, city) VALUES
('PL-01', 'Poland', 'Katowice'),
('PL-02', 'Poland', 'Gdansk'),
('US-01', 'USA', 'New York'),
('DE-01', 'Germany', 'Frankfurt');

