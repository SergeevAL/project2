
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

-- select * from public.shipping;

--------------------
-- create tables
--------------------

drop table public.shipping_country_rates cascade;
drop table public.shipping_agreement cascade;
drop table public.shipping_transfer cascade;
drop table public.shipping_info cascade;
drop table public.shipping_status cascade;


create table if not exists public.shipping_country_rates (
	shipping_country_id serial,
	shipping_country text,
	shipping_country_base_rate double precision not null check (shipping_country_base_rate >= 0),
	primary key (shipping_country_id)
);
comment on column public.shipping_country_rates.shipping_country is 'страна доставки'; 
comment on column public.shipping_country_rates.shipping_country_base_rate is 'налог на доставку в страну, который является процентом от стоимости payment_amount'; 

----------------

create table if not exists public.shipping_agreement (
	agreementid bigint,
	agreement_number text,
	agreement_rate double precision not null check (agreement_rate >= 0),
	agreement_commission double precision not null check (agreement_commission >= 0),
	primary key (agreementid)
);
comment on column public.shipping_agreement.agreement_number is 'agreement_number — номер договора в бухгалтерии'; 
comment on column public.shipping_agreement.agreement_rate is 'ставка налога за стоимость доставки товара для вендора'; 
comment on column public.shipping_agreement.agreement_commission is 'комиссия, то есть доля в платеже являющаяся доходом компании от сделки';


------------------

create table if not exists public.shipping_transfer (
	transfer_type_id serial,
	transfer_type char(2),
	transfer_model text,
	shipping_transfer_rate double precision not null check (shipping_transfer_rate >= 0),
	primary key (transfer_type_id)
);
comment on column public.shipping_transfer.transfer_type is 'тип доставки. 1p означает, что компания берёт ответственность за доставку на себя, 3p — что за отправку ответственен вендор.'; 
comment on column public.shipping_transfer.transfer_model is 'модель доставки, то есть способ, которым заказ доставляется до точки: car — машиной, train — поездом, ship — кораблем, airplane — самолетом, multiple — комбинированной доставкой.'; 
comment on column public.shipping_transfer.shipping_transfer_rate is 'процент стоимости доставки для вендора в зависимости от типа и модели доставки, который взимается интернет-магазином для покрытия расходов.';

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
comment on column public.shipping_info.vendorid is 'идентификатор вендора'; 
comment on column public.shipping_info.payment_amount is 'уплаченная сумма по заказу'; 
comment on column public.shipping_info.shipping_plan_datetime is 'заявленная дата доставки';


---------------------

create table if not exists public.shipping_status (
	shippingid bigint,
	status text,
	state text,
	shipping_start_fact_datetime TIMESTAMP,
	shipping_end_fact_datetime TIMESTAMP,
	primary key (shippingid)
);
comment on column public.shipping_status.status is 'статус доставки в таблице shipping по данному shippingid. Может принимать значения in_progress — доставка в процессе, либо finished — доставка завершена'; 
comment on column public.shipping_status.state is 'промежуточные точки заказа, которые изменяются в соответствии с обновлением информации о доставке по времени state_datetime'; 
comment on column public.shipping_status.shipping_start_fact_datetime is 'shipping_start_fact_datetime — это время state_datetime, когда state заказа перешёл в состояние booked';
comment on column public.shipping_status.shipping_end_fact_datetime is 'shipping_end_fact_datetime — это время state_datetime , когда state заказа перешёл в состояние received';

---------------------
-- inserts
---------------------

--select * from public.shipping s;

---------------------

truncate table public.shipping_country_rates cascade;
truncate table public.shipping_agreement cascade;
truncate table public.shipping_transfer cascade;
truncate table public.shipping_info cascade;
truncate table public.shipping_status cascade;

---------------------
--  Здесь не совсем понятно, у поля shipping_country_id тип serial, и при инсерте он инкрементирует новый shipping_country_id	
insert into public.shipping_country_rates
(shipping_country, shipping_country_base_rate)
select distinct s.shipping_country, s.shipping_country_base_rate
from public.shipping s;

-- select * from public.shipping_country_rates;

--------------------

insert into public.shipping_agreement
(agreementid, agreement_number, agreement_rate, agreement_commission)
select ss.vendor_agreement_description[1]::bigint agreementid,
	ss.vendor_agreement_description[2]::text agreement_number, 
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
select distinct ss.shippingid,
	ss.vendorid, 
	ss.payment_amount,
	ss.shipping_plan_datetime,
	st.transfer_type_id,
	scr.shipping_country_id,
	sa.agreementid
from public.shipping ss
join public.shipping_country_rates scr using (shipping_country)
join public.shipping_transfer st on ((st.transfer_type || ':' || st.transfer_model) = ss.shipping_transfer_description)
join public.shipping_agreement sa on ((sa.agreementid || ':'|| sa.agreement_number || ':' || sa.agreement_rate || ':' || sa.agreement_commission) = ss.vendor_agreement_description);

-- select * from public.shipping_info;

--------------------

insert into public.shipping_status
(shippingid, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
with
ssp as (
	select shippingid,
		max(case when state = 'booked' then state_datetime else null end) as shipping_start_fact_datetime,
		max(state_datetime) as state_datetime
	from public.shipping
	where state in ('booked', 'recieved')
	group by shippingid)
select ssm.shippingid,
	ssm.status, 
	ssm.state,
	ssp.shipping_start_fact_datetime,
	ssm.state_datetime as shipping_end_fact_datetime
from public.shipping ssm
join ssp using (shippingid, state_datetime)
order by 1;

-- select * from public.shipping_status;
--------------------
-- Views
--------------------

-- drop view public.shipping_datamart cascade;

---------------------
create view public.shipping_datamart as
select info.shippingid,
	info.vendorid,
	stf.transfer_type,
	(st.shipping_end_fact_datetime - st.shipping_start_fact_datetime) as full_day_at_shipping,
	case when (st.shipping_end_fact_datetime > info.shipping_plan_datetime) then 1 else 0 end as is_delay,
	case when st.status = 'finished' then 1 else 0 end as is_shipping_finish,
	case when (st.shipping_end_fact_datetime > info.shipping_plan_datetime) then date_part('day', age(st.shipping_end_fact_datetime, info.shipping_plan_datetime)) else 0 end as delay_day_at_shipping,
	info.payment_amount,
	info.payment_amount * ( sr.shipping_country_base_rate + sa.agreement_rate + stf.shipping_transfer_rate) as vat,
	info.payment_amount * sa.agreement_commission as profit
from public.shipping_info info
join shipping_status st using (shippingid)
join public.shipping_transfer stf using (transfer_type_id)
join public.shipping_agreement sa using (agreementid)
join public.shipping_country_rates sr using (shipping_country_id);
--------

--select * from public.shipping_datamart;

---- Теперь shipping_status работает, view не нужна) 

-- По вопросу: почему используется именно view в самом конце.
-- Я думаю что тут несколько пунктов основных, первое это для расширения или внесения изменений в структуру витрины
-- если появятся новые справочники можно будет их подцепить просто внеся изменения во view.
-- Второе это возможность отображения источника который постоянно или часто обновляется.
