/*
7.A monthly, reduced fact table DDL `host_activity_reduced`
- month
- host
- hit_array - think COUNT(1)
- unique_visitors array -  think COUNT(DISTINCT user_id)
*/

CREATE TABLE host_activity_reduced (
    host TEXT,
    month DATE,
    hit_array REAL[], 
    unique_visitors REAL[],
    PRIMARY KEY(host, month)
);