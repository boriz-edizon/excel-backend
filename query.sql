create table file (
	id int not null auto_increment,
    email_id varchar(255),
    name varchar(255),
    country varchar(255),
    state varchar(255),
    city varchar(255),
    telephone_number varchar(255),
    address_line_1 varchar(255),
    address_line_2 varchar(255),
    date_of_birth varchar(255),
    gross_salary_FY2019_20 varchar(255),
    gross_salary_FY2020_21 varchar(255),
    gross_salary_FY2021_22 varchar(255),
    gross_salary_FY2022_23 varchar(255),
    gross_salary_FY2023_24 varchar(255),
	PRIMARY KEY (id)
);
Truncate TABLE file;
select count(*) from file;
drop table file;


update file set country = replace(country,"UK","mumbai");

SET SQL_SAFE_UPDATES = 0;
