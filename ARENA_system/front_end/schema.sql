-- store cookie
create table IF NOT EXISTS USER
(
    uid int primary key
);
INSERT or IGNORE into USER
values (10000);

create table if NOT EXISTS SQL
(
    hash char(64),
    sid  int unique,
    sql  text,
    conf text,
    primary key (hash)
);

-- cache the result of each sql
create table if not exists QUERY
(
    sid    integer,
    nid    int,
    result text,
    foreign key (sid) references SQL (sid)
);

-- store the information of database
create table if not exists DBINFO
(
    name varchar(64),
    info text
);

create table if not exists SUGGEST
(
    id   integer primary key autoincrement,
    uid  integer,
    info text
);

-- used for I-TIPS, record the process id corresponding to the query
CREATE TABLE IF NOT EXISTS process
(
    pid int primary key,
    fileName text
);
