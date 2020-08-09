USE DATABASE DVAZ1STG;
USE SCHEMA SCH_LD2;

DROP TABLE IF EXISTS avH
; DROP TABLE IF EXISTS wssLav
; DROP TABLE IF EXISTS avS
;

CREATE TABLE avH
(
src VARCHAR(40),
avhk CHAR(32),
stratset VARCHAR(1023),
wgtnum BIGINT
);

CREATE TABLE wssLav
(
src VARCHAR(40),
wssLavhk CHAR(32),
wsshk CHAR(32),
avhk CHAR(32)
);

CREATE TABLE avS
(
src VARCHAR(40),
avhk CHAR(32),
avhd CHAR(32),
winrt VARCHAR(30),
execcnt VARCHAR(12),
updepc VARCHAR(24)
);
