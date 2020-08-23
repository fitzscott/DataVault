-- in biz schema
DROP TABLE IF EXISTS STRATEGY_H ;

CREATE TABLE STRATEGY_H 
(
                STRATEGY_HASH_KEY VARCHAR(32) NOT NULL,
                LOAD_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                RECORD_SRC VARCHAR(40),
                STRATEGY_ID VARCHAR(50) NOT NULL,
                primary key (STRATEGY_HASH_KEY)
);

INSERT INTO STRATEGY_H
SELECT 
                h.STRATEGY_HASH_KEY,
                h.LOAD_DT,
                h.RECORD_SRC,
                CAST(s.STRATEGY_TXT AS VARCHAR(50))
FROM "DVAZ1"."SCH_TGT1"."STRATEGY_H" h
    JOIN "DVAZ1"."SCH_TGT1"."STRATEGY_S" s
    ON h.STRATEGY_HASH_KEY = s.STRATEGY_HASH_KEY
;

INSERT INTO STRATEGY_H
SELECT 
                STRATEGY_HASH_KEY,
                LOAD_DT,
                RECORD_SRC,
                STRATEGY_ID
FROM "DVAZ1"."SCH_TGT2"."STRATEGY_H"
;

SELECT *
FROM STRATEGY_H;

DROP TABLE IF EXISTS Strategy_SameAs_L;

CREATE TABLE Strategy_SameAs_L
(
  Strategy_SameAs_Hash_Key CHAR(32),
  Primary_Strategy_Hash_Key CHAR(32),
  Duplicate_Strategy_Hash_Key CHAR(32),
  Primary Key(Strategy_SameAs_Hash_Key)
);

SELECT 
    pri.Strategy_Hash_Key,
    pri.Record_Src,
    pri.Strategy_ID,
    dup.Strategy_Hash_Key,
    dup.Record_Src,
    dup.Strategy_ID
FROM STRATEGY_H pri
    JOIN STRATEGY_H dup
    ON pri.STRATEGY_ID = dup.STRATEGY_ID
WHERE pri.RECORD_SRC <> dup.RECORD_SRC
    AND pri.RECORD_SRC = 'Win10.laptop.Big1'
ORDER BY 3,5;

/* Problems - records like:
Ubuntu.Laptop.Big1 vs. Ubuntu.laptop.Big1
Also need to figure out HT get unique set. */

INSERT INTO Strategy_SameAs_L
SELECT
    MD5(CONCAT(pri.STRATEGY_ID, '|', pri.Record_src, '|', dup.STRATEGY_ID, '|', dup.Record_src)) sahk,
    pri.Strategy_Hash_Key,
    dup.Strategy_Hash_Key
FROM STRATEGY_H pri
    JOIN STRATEGY_H dup
    ON pri.STRATEGY_ID = dup.STRATEGY_ID
WHERE pri.RECORD_SRC <> dup.RECORD_SRC
    AND pri.RECORD_SRC = 'Win10.laptop.Big1'
;

SELECT DISTINCT h.*
FROM STRATEGY_H h
    LEFT OUTER JOIN Strategy_SameAs_L ssal
    ON h.strategy_hash_key = ssal.Duplicate_Strategy_Hash_Key
WHERE ssal.Duplicate_Strategy_Hash_Key IS NULL
;

SELECT *
FROM Strategy_SameAs_L ssal
    JOIN Strategy_H h
    ON ssal.Primary_Strategy_Hash_Key = h.Strategy_Hash_Key
;

SELECT *
FROM Strategy_SameAs_L ssal
    JOIN DVAZ1.SCH_TGT1.Strategy_H h
    ON ssal.Primary_Strategy_Hash_Key = h.Strategy_Hash_Key
    JOIN DVAZ1.SCH_TGT1.Strategy_S s
    ON h.Strategy_Hash_Key = s.Strategy_Hash_Key
;
