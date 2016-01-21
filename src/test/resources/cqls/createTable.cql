create columnfamily if not exists user (
    userid text,
    fname text,
    lname text,
    email  text,
    primary key(userid) );
