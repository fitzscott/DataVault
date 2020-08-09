USE WAREHOUSE WH_STAGE_1;
USE DATABASE DVAZ1STG;
USE SCHEMA SCH_LD2;

DROP TABLE IF EXISTS grH;
DROP TABLE IF EXISTS grS;
DROP TABLE IF EXISTS ssH;
DROP TABLE IF EXISTS wssH;
DROP TABLE IF EXISTS wssLgr;
DROP TABLE IF EXISTS ssLwss;
DROP TABLE IF EXISTS sH;
DROP TABLE IF EXISTS sLssm;
DROP TABLE IF EXISTS ssmH;
DROP TABLE IF EXISTS ssLssm;

CREATE TABLE grH
(
src VARCHAR(40),
grhk CHAR(32),
gameid BIGINT,
posnum SMALLINT
);

CREATE TABLE grS
(
src VARCHAR(40),
grhk CHAR(32),
grhd CHAR(32),
score INT,
srank SMALLINT,
winloss CHAR(1)
);

CREATE TABLE ssH
(
src VARCHAR(40),
sshk CHAR(32),
stratset VARCHAR(1023)
);

CREATE TABLE wssH
(
src VARCHAR(40),
wsshk CHAR(32),
stratset VARCHAR(1023),
wgtsum BIGINT
);

CREATE TABLE wssLgr
(
src VARCHAR(40),
wssLgrhk CHAR(32),
wsshk CHAR(32),
grhk CHAR(32)
);

CREATE TABLE ssLwss
(
src VARCHAR(40),
ssLwsshk CHAR(32),
sshk CHAR(32),
wsshk CHAR(32)
);

CREATE TABLE ssmH
(
src VARCHAR(40),
ssmhk CHAR(32),
strat VARCHAR(50),
stratset VARCHAR(1023)
);

CREATE TABLE ssLssm
(
src VARCHAR(40),
ssLssmhk CHAR(32),
sshk CHAR(32),
ssmhk CHAR(32)
);

CREATE TABLE sH
(
src VARCHAR(40),
shk CHAR(32),
strat VARCHAR(50)
);

CREATE TABLE sLssm
(
src VARCHAR(40),
sLssmhk CHAR(32),
shk CHAR(32),
ssmhk CHAR(32)
);