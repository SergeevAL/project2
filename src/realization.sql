
----------------
--  Загрузка CSV в shipping
----------------

COPY public.shipping (
   shippingid,
   saleid,
   orderid,
   clientid,
   payment_amount,
   state_datetime,
   productid,
   description,
   vendorid,
   namecategory,
   base_country,
   status,
   state,
   shipping_plan_datetime,
   hours_to_plan_shipping,
   shipping_transfer_description,
   shipping_transfer_rate,
   shipping_country,
   shipping_country_base_rate,
   vendor_agreement_description)
FROM '/lessons/shipping.csv'
DELIMITER ','
CSV HEADER;

--------------------
-- create tables
--------------------

-- drop table public.shipping_country_rates cascade;
-- drop table public.shipping_agreement cascade;
-- drop table public.shipping_transfer cascade;
-- drop table public.shipping_info cascade;
-- drop table public.shipping_status cascade;


create table if not exists public.shipping_country_rates (
	shipping_country_id serial,
	shipping_country text,
	shipping_country_base_rate double precision,
	primary key (shipping_country_id)
);

----------------

create table if not exists public.shipping_agreement (
	agreementid bigint,
	agreement_number text,
	agreement_rate double precision,
	agreement_commission double precision,
	primary key (agreementid)
);

------------------

create table if not exists public.shipping_transfer (
	transfer_type_id serial,
	transfer_type text,
	transfer_model text,
	shipping_transfer_rate double precision,
	primary key (transfer_type_id)
);

---------------------

create table if not exists public.shipping_info (
	shippingid bigint,
	vendorid bigint,
	payment_amount bigint,
	shipping_plan_datetime TIMESTAMP,
	transfer_type_id bigint,
	shipping_country_id bigint,
	agreementid bigint,
	primary key (shippingid),
	foreign key (transfer_type_id) REFERENCES public.shipping_transfer(transfer_type_id) ON UPDATE cascade,
	foreign key (shipping_country_id) REFERENCES public.shipping_country_rates(shipping_country_id) ON UPDATE cascade,
	foreign key (agreementid) REFERENCES public.shipping_agreement(agreementid) ON UPDATE cascade
);

---------------------

create table if not exists public.shipping_status (
	shippingid bigint,
	status text,
	state text,
	shipping_start_fact_datetime TIMESTAMP,
	shipping_end_fact_datetime TIMESTAMP,
	primary key (shippingid)
);

---------------------
-- inserts
---------------------

-- select * from public.shipping s;

---------------------

-- truncate table public.shipping_country_rates cascade;
-- truncate table public.shipping_agreement cascade;
-- truncate table public.shipping_transfer cascade;
-- truncate table public.shipping_info cascade;
-- truncate table public.shipping_status cascade;

---------------------

insert into public.shipping_country_rates
(shipping_country, shipping_country_base_rate)
select distinct s.shipping_country, s.shipping_country_base_rate
from public.shipping s;

-- select * from public.shipping_country_rates;

--------------------

insert into public.shipping_agreement
(agreementid, agreement_number, agreement_rate, agreement_commission)
select cast(ss.vendor_agreement_description[1] as bigint) agreementid,
	cast(ss.vendor_agreement_description[2] as text) agreement_number, 
	cast(ss.vendor_agreement_description[3] as double precision) agreement_rate, 
	cast(ss.vendor_agreement_description[4] as double precision) agreement_commission 
from (select distinct regexp_split_to_array(s.vendor_agreement_description, ':+') as vendor_agreement_description
from public.shipping s) ss;

-- select * from public.shipping_agreement;

--------------------

insert into public.shipping_transfer
(transfer_type, transfer_model, shipping_transfer_rate)
select ss.shipping_transfer_description[1] transfer_type,
	ss.shipping_transfer_description[2] transfer_model,
	ss.shipping_transfer_rate shipping_transfer_rate
from (select distinct regexp_split_to_array(s.shipping_transfer_description, ':+') as shipping_transfer_description,
	 s.shipping_transfer_rate
from public.shipping s) ss;

-- select * from public.shipping_transfer;

--------------------

insert into public.shipping_info
(shippingid, vendorid, payment_amount, shipping_plan_datetime, transfer_type_id, shipping_country_id, agreementid)
with 
scr as (
select *
from public.shipping_country_rates
),
sa as (
select *
from public.shipping_agreement
),
st as (
select *
from public.shipping_transfer
)
select distinct ss.shippingid,
	ss.vendorid, 
	ss.payment_amount,
	ss.shipping_plan_datetime,
	st.transfer_type_id,
	scr.shipping_country_id,
	sa.agreementid
from public.shipping ss
join scr on (scr.shipping_country = ss.shipping_country and scr.shipping_country_base_rate = ss.shipping_country_base_rate)
join st on ((st.transfer_type || ':' || st.transfer_model) = ss.shipping_transfer_description)
join sa on ((sa.agreementid || ':'|| sa.agreement_number || ':' || sa.agreement_rate || ':' || sa.agreement_commission) = ss.vendor_agreement_description);

-- select * from public.shipping_info;

--------------------

insert into public.shipping_status
(shippingid, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
with
ssm as (
select sh.shippingid, sh.status, sh.state, sh.state_datetime
from public.shipping sh
join (select shippingid, max(state_datetime) as state_datetime
from public.shipping
group by shippingid) sm
on sh.shippingid = sm.shippingid and sh.state_datetime = sm.state_datetime
),
ssst as (
select distinct  shippingid, state_datetime as shipping_start_fact_datetime, state
from public.shipping
where state = 'booked'
),
ssen as (
select distinct shippingid, state_datetime as shipping_end_fact_datetime, state
from public.shipping
where state = 'recieved'
)
select ssm.shippingid,
	ssm.status, 
	ssm.state,
	ssst.shipping_start_fact_datetime,
	ssen.shipping_end_fact_datetime
from ssm
join ssst on (ssm.shippingid = ssst.shippingid)
join ssen on (ssst.shippingid = ssen.shippingid)
order by 1;

-- select * from public.shipping_status;
--------------------
-- Views
--------------------

-- drop view public.shipping_datamart cascade;

---------------------
create view if not exists public.shipping_datamart as
with 
info as (
select *
from public.shipping_info
),
st as (
--select * from public.v_shipping_status
select * from public.shipping_status
),
pl as (
select shippingid, max(shipping_plan_datetime) as shipping_plan_datetime
from public.shipping
group by shippingid
) 
select info.shippingid,
	info.vendorid,
	stf.transfer_type,
	(st.shipping_end_fact_datetime - st.shipping_start_fact_datetime) as full_day_at_shipping,
	case when (st.shipping_end_fact_datetime > info.shipping_plan_datetime) then 1 else 0 end as is_delay,
	case when st.status = 'finished' then 1 else 0 end as is_shipping_finish,
	case when (st.shipping_end_fact_datetime > pl.shipping_plan_datetime) then date_part('day', age(st.shipping_end_fact_datetime, pl.shipping_plan_datetime)) else 0 end as delay_day_at_shipping,
	info.payment_amount,
	info.payment_amount * ( sr.shipping_country_base_rate + sa.agreement_rate + stf.shipping_transfer_rate) as vat,
	info.payment_amount * sa.agreement_commission as profit
from info
join st on (info.shippingid = st.shippingid)
join public.shipping_transfer stf on (stf.transfer_type_id = info.transfer_type_id)
join public.shipping_agreement sa on (sa.agreementid = info.agreementid)
join public.shipping_country_rates sr on (sr.shipping_country_id = info.shipping_country_id)
join pl on (info.shippingid = pl.shippingid);
--------


---- Так как инcерт на shipping_status не работает корректно, создал view для себя)
create view public.v_shipping_status as
with
ssm as (
select sh.shippingid, sh.status, sh.state, sh.state_datetime
from public.shipping sh
join (select shippingid, max(state_datetime) as state_datetime
from public.shipping
group by shippingid) sm
on sh.shippingid = sm.shippingid and sh.state_datetime = sm.state_datetime
),
ssst as (
select distinct  shippingid, state_datetime as shipping_start_fact_datetime, state
from public.shipping
where state = 'booked'
),
ssen as (
select distinct shippingid, state_datetime as shipping_end_fact_datetime, state
from public.shipping
where state = 'recieved'
)
select ssm.shippingid,
	ssm.status, 
	ssm.state,
	ssst.shipping_start_fact_datetime,
	ssen.shipping_end_fact_datetime
from ssm
join ssst on (ssm.shippingid = ssst.shippingid)
join ssen on (ssst.shippingid = ssen.shippingid)
order by 1;

-- select * from public.v_shipping_status;
