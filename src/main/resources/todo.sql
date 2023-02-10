create schema accounts_hidden_dev;

create table accounts_hidden_dev.hidden_account
(
    account_number varchar(25) not null
        primary key,
    created        timestamp   not null,
    cause          varchar(500)
);
alter table accounts_hidden_dev.hidden_account owner to accounts_hidden_dev;

create table accounts_hidden_dev.hidden_card
(
    ucid      numeric(38) not null
    primary key,
    origin    varchar(128),
    hide_date timestamp
);
alter table accounts_hidden_dev.hidden_card owner to accounts_hidden_dev;

