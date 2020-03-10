create table agency
(
	id SMALLINT auto_increment primary key,
	name VARCHAR(200) not null,
	abbreviation VARCHAR(100) not null,
	url VARCHAR(200),
	logo BLOB(10000000)
);

insert into agency (name, abbreviation, url) values ('Default agency', 'DEF', 'https://www.example.org/');

create unique index agency_abbreviation_uindex
	on agency (abbreviation);

create unique index agency_name_uindex
	on agency (name);

alter table crawl
    add agency_id SMALLINT null;

alter table crawl
    add constraint crawl_agency_id_fk
        foreign key (agency_id) references agency (id);

alter table crawl_series
    add agency_id SMALLINT null;

alter table crawl_series
    add constraint crawl_series_agency_id_fk
        foreign key (agency_id) references agency (id);