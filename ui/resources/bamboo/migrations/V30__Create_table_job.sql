create table task
(
  id varchar(64) not null primary key,
  name varchar(100) not null,
  enabled tinyint(1) default 1 not null,
  start_time datetime,
  finish_time datetime
);
