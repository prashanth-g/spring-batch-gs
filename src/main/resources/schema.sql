DROP TABLE IF EXISTS PEOPLE;

CREATE TABLE PEOPLE (
    id          BIGINT(10)      NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email       VARCHAR (255)   NOT NULL,
    age         INT (3)         NOT NULL,
    first_name  VARCHAR(255)    NOT NULL
);