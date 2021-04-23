"""
CREATE TABLE IF NOT EXISTS tick(
    time			 	timestamp with time zone 			NOT NULL,
	contract_id			integer								NOT NULL,
	symbol				varchar(15)							DEFAULT '',
    price 				double precision 					NOT NULL,
    volume 				integer 							NOT NULL,
    special_conditions 	VARCHAR(50)							DEFAULT '',
    unreported 			boolean 							DEFAULT false,
    past_limit 			boolean 							DEFAULT false,
    PRIMARY KEY (time, contract_id)
    CONSTRAINT fkey_contract
        FOREIGN KEY (contract_id) 
	    REFERENCES contract(contract_id)
	    ON DELETE CASCADE
	    ON UPDATE CASCADE
)
--Timescale specific statements to create hypertables for better performance
SELECT create_hypertable('tick', 'time'); 
--Drop current foreign key and define a new foreign key
ALTER TABLE tick
DROP CONSTRAINT fkey_contract;


ALTER TABLE tick
ADD CONSTRAINT fkey_contract
FOREIGN KEY (contract_id)
REFERENCES contract(contract_id)
ON DELETE CASCADE
ON UPDATE CASCADE


-- Select the first and last tick for everyday
select date(time), min(time), max(time) from tick
where symbol = 'PLS' AND time between '2020-06-01 10:06:30' and '2021-3-19 10:07:00'
GROUP BY date(time)



CREATE TABLE bar(
	time				TIMESTAMP			NOT NULL,
	open				DOUBLE PRECISION	NOT NULL,
	high				DOUBLE PRECISION	NOT NULL,
	low					DOUBLE PRECISION	NOT NULL,
	close				DOUBLE PRECISION	NOT NULL,
	average				DOUBLE PRECISION	NOT NULL,
	volume				INTEGER				NOT NULL,
	bar_count			INTEGER,
	symbol 				VARCHAR(10)			NOT NULL,
	contract_id			INTEGER				NOT NULL,
	primary_exchange	VARCHAR(15)			NOT NULL
	PRIMARY KEY (time, contract_id)
	CONSTRAINT fk_contract
        FOREIGN KEY (contract_id) 
	    REFERENCES contract(contract_id)
	    ON DELETE CASCADE
	    ON UPDATE CASCADE
);
--Timescale specific statements to create hypertables for better performance
SELECT create_hypertable('bar', 'time');
CREATE INDEX on bar (ticker_id, time DESC);
INSERT INTO bar(
	time, open, high, low, close, average, volume, bar_count, ticker, ticker_id, primary_exchange) 
    VALUES 
    ('2020-07-15 19:23:34', 14.50, 14.50, 14.50, 14.50, 14.50, 260, 0, 'A2M', '24545145', 'ASX'),
    ('2020-08-15 19:23:34', 14.50, 14.50, 14.50, 14.50, 14.50, 260, 0, 'A2M', '24545145', 'ASX'),
    ('2020-09-15 19:23:34', 14.50, 14.50, 14.50, 14.50, 14.50, 260, 0, 'A2M', '24545145', 'ASX');


create table if not exists contract(
    contract_id             integer             not null,
	symbol                  varchar(10)         not null,
	full_name               varchar(30)         not null,
	security_type           varchar(10)         not null,
	security_id             integer             default 0,
	security_id_type        varchar(20)         default '',
	stock_type              varchar(10)         default '',
	currency                varchar(5)          not null,
    primary_exchange        varchar(10)         not null,
	exchange                varchar(10)         default '',
	valid_exchanges         varchar(30)         default '',
	sector                  varchar(20)         default '',
	category                varchar(20)         default '',
	sub_category            varchar(30)         default '',
	time_zone               varchar(20)         default '',
	strike_price            double precision    default 0.0,
	min_tick                double precision    default 0.0,
	earliest_avbl_dt        timestamp with time zone,
	org_id                  integer             NOT NULL
	PRIMARY KEY (contract_id)
    CONSTRAINT fkey_org
        FOREIGN KEY (org_id)
        REFERENCES org(org_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);
-- Do not have to create hyper table as contract is not time series related data


CREATE TABLE IF NOT EXISTS auction(
	auc_type			varchar(20)				not null,
	contract_id 		integer 				not null,
	symbol				varchar(20)				not null,
	time				timestamptz				not null,
	price				double precision		not null,
	volume				double precision		not null,
	trade				integer					not null,
	money_vol			double precision		not null,
	exchange			varchar(20)				not null,
	PRIMARY KEY (time, contract_id),
	CONSTRAINT fkey_contract
        FOREIGN KEY (contract_id)
	    REFERENCES contract(contract_id)
	    ON DELETE CASCADE
	    ON UPDATE CASCADE
);
SELECT create_hypertable('auction', 'time');




CREATE TABLE IF NOT EXISTS fundamental_xmls
(
    contract_id             integer                 not null,
    org_id                  integer                 not null,
	name                    text                    not null,
	snapshot_xml            text,
	fin_stmts_xml           text,
	fin_summary_xml         text,
	ratios_xml              text,
	research_xml            text,
    PRIMARY KEY (org_id)
    CONSTRAINT fk_contract
        FOREIGN KEY (contract_id) 
	    REFERENCES contract(contract_id)
	    ON DELETE CASCADE
	    ON UPDATE CASCADE
)


UPDATE contract
SET org_id = fundamental_xmls.org_id
FROM fundamental_xmls
WHERE contract.contract_id = fundamental_xmls.contract_id



CREATE TABLE IF NOT EXISTS xml_snapshot(
date			timestamptz 		not null,
contract_id		integer				not null,
symbol 			varchar(15),
xml_snapshot	text				not null,
CONSTRAINT xml_snapshot_pkey 
	PRIMARY KEY (date, contract_id),
CONSTRAINT fkey_contract 
	FOREIGN KEY (contract_id)
	REFERENCES contract(contract_id) 
	ON UPDATE CASCADE
	ON DELETE CASCADE
)

CREATE TABLE IF NOT EXISTS xml_financial_stmts(
date					timestamptz 		not null,
contract_id				integer				not null,
symbol 					varchar(15),
xml_financial_stmts		text				not null,
CONSTRAINT xml_financial_stmts_pkey 
	PRIMARY KEY (date, contract_id),
CONSTRAINT fkey_contract 
	FOREIGN KEY (contract_id)
	REFERENCES contract(contract_id) 
	ON UPDATE CASCADE
	ON DELETE CASCADE
)

CREATE TABLE IF NOT EXISTS xml_research(
date						timestamptz 		not null,
contract_id					integer				not null,
symbol 						varchar(15),
xml_ratios					text				not null,
CONSTRAINT xml_research_pkey 
	PRIMARY KEY (date, contract_id),
CONSTRAINT fkey_contract 
	FOREIGN KEY (contract_id)
	REFERENCES contract(contract_id) 
	ON UPDATE CASCADE
	ON DELETE CASCADE
)


CREATE TABLE IF NOT EXISTS share_split
(
	org_id 					integer 				NOT NULL,
	date 					date 					NOT NULL,
    number 				double precision 		NOT NULL,
    CONSTRAINT split_pkey PRIMARY KEY (date, org_id),
    CONSTRAINT fkey_org FOREIGN KEY (org_id)
        REFERENCES org(org_id) 
        ON UPDATE CASCADE
        ON DELETE CASCADE
);
SELECT create_hypertable('share_split', 'date');


CREATE TABLE IF NOT EXISTS share_issued
(
	org_id 					integer 				NOT NULL,
	date 					date 					NOT NULL,
    number 				double precision 		NOT NULL,
    CONSTRAINT issued_pkey PRIMARY KEY (date, org_id),
    CONSTRAINT fkey_org FOREIGN KEY (org_id)
        REFERENCES org(org_id) 
        ON UPDATE CASCADE
        ON DELETE CASCADE
);
SELECT create_hypertable('share_issued', 'date');


CREATE TABLE IF NOT EXISTS share_float
(
	org_id 					integer 				NOT NULL,
	date 					date 					NOT NULL,
    number 				double precision 		NOT NULL,
    CONSTRAINT float_pkey PRIMARY KEY (date, org_id),
    CONSTRAINT fkey_org FOREIGN KEY (org_id)
        REFERENCES org(org_id) 
        ON UPDATE CASCADE
        ON DELETE CASCADE
);
SELECT create_hypertable('share_float', 'date');


"""
