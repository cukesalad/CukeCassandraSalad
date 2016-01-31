create columnfamily if not exists user (
    userid text,
    fname text,
    lname text,
    email  text,
    some int,
    primary key(userid) );
