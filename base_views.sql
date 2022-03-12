
------------------------------------
-- V_BLOCK
------------------------------------
create or replace view v_block 
as
select b.*, ph.view as pool_hash, ph.id as pool_hash_id
from block b
	inner join slot_leader sl on sl.id = b.slot_leader_id
	left join pool_hash ph on ph.id = sl.pool_hash_id;

------------------------------------
-- V_POOL_UPDATES
------------------------------------

drop view if exists v_pool_update cascade;

create or replace view v_pool_update
as
select 
	pool_id,
	pool_hash_id,
	epoch_no,
	slot_no,
	epoch_slot_no,
	time,
	pledge,
	active_epoch_no,
	meta_id,
	margin,
	fixed_cost,
	reward_addr_id,
	reward_addr_hash,
	cert_index,
	vrf_key_hash,
	registered_tx_id,
	case when rownum = 1 then 1 else 0 end as is_epoch_final,
	case when (row_number() over (partition by pool_id order by epoch_no desc) ) = 1 then 1 else 0 end as is_current
from (
	select 
		ph.view as pool_id,
		pu.hash_id as pool_hash_id,
		b.epoch_no,
		b.slot_no,
		b.epoch_slot_no,
		b.time,
		row_number() over (partition by ph.view, b.epoch_no order by b.time desc ) as rownum,
		pu.pledge,
		pu.active_epoch_no,
		pu.meta_id,
		pu.margin,
		pu.fixed_cost,
		sa.id as reward_addr_id,
		cert_index,
		vrf_key_hash,
		pu.registered_tx_id,
		right(encode(pu.reward_addr, 'hex'), 56) as reward_addr_hash
	from pool_update pu
		inner join tx t on pu.registered_tx_id = t.id
		inner join block b on t.block_id = b.id
		inner join pool_hash ph on pu.hash_id = ph.id
		--left join stake_address sa on right(encode(pu.reward_addr, 'hex'), 56) = sa.view
		left join stake_address sa on pu.reward_addr = sa.hash_raw
) a;

-----------------------------------------------
-- V_POOL_RETIRE
-----------------------------------------------

drop view if exists v_pool_retire cascade;

create or replace view v_pool_retire
as
select 
	ph.view as pool_id,
	encode(ph.hash_raw, 'hex') as hash,
	pr.hash_id as pool_hash_id,
	b.epoch_no,
	b.slot_no,
	b.epoch_slot_no,
	b.time,	
	pr.retiring_epoch,
	pr.announced_tx_id,
	pr.cert_index,
	row_number() over (partition by ph.view, b.epoch_no order by b.time desc ) as rownum
from pool_retire pr
inner join tx t on pr.announced_tx_id = t.id
		inner join block b on t.block_id = b.id
		inner join pool_hash ph on pr.hash_id = ph.id;


-----------------------------------------------
-- V_POOL_HISTORY_BY_EPOCH
-----------------------------------------------

drop view if exists v_pool_history_by_epoch cascade;

create or replace view v_pool_history_by_epoch
as
with epoch_pools as (
	select e.epoch_no, p.pool_hash_id
	from (
		select distinct epoch_no
		from epoch_param ep
	) e
	cross join (
		select pool_hash_id, min(epoch_no) as epoch_no
		from v_pool_update
		group by pool_hash_id
	)  p 
	where p.epoch_no <= e.epoch_no 
)
-- the following crazy syntax is to fill in empty epochs where no changes have happened in a pool
-- and working around the fact that postgreSQL does not support 'ignore nulls' in window functions
select 
	epoch_no,
	pool_hash_id,
	case when registered_margin is not null then 1 else 0 end as is_registered_epoch,
	has_announced_retirement,
	retiring_epoch,
	FIRST_VALUE(pool_id) OVER (PARTITION BY pool_hash_id, registered_partition) AS pool_id,
	FIRST_VALUE(registered_margin) OVER (PARTITION BY pool_hash_id, registered_partition) AS registered_margin,
	FIRST_VALUE(registered_pledge) OVER (PARTITION BY pool_hash_id, registered_partition) AS registered_pledge,
	FIRST_VALUE(registered_fixed_cost) OVER (PARTITION BY pool_hash_id, registered_partition) AS registered_fixed_cost,
	FIRST_VALUE(registered_cert_index) OVER (PARTITION BY pool_hash_id, registered_partition) AS registered_cert_index,
	FIRST_VALUE(registered_vrf_key_hash) OVER (PARTITION BY pool_hash_id, registered_partition) AS registered_vrf_key_hash,
	FIRST_VALUE(registered_reward_addr) OVER (PARTITION BY pool_hash_id, registered_partition) AS registered_reward_addr,
	FIRST_VALUE(active_margin) OVER (PARTITION BY pool_hash_id, active_partition) AS active_margin,
	FIRST_VALUE(active_pledge) OVER (PARTITION BY pool_hash_id, active_partition) AS active_pledge,
	FIRST_VALUE(active_fixed_cost) OVER (PARTITION BY pool_hash_id, active_partition) AS active_fixed_cost,
	FIRST_VALUE(active_cert_index) OVER (PARTITION BY pool_hash_id, active_partition) AS active_cert_index,
	FIRST_VALUE(active_vrf_key_hash) OVER (PARTITION BY pool_hash_id, active_partition) AS active_vrf_key_hash,
	FIRST_VALUE(active_reward_addr) OVER (PARTITION BY pool_hash_id, active_partition) AS active_reward_addr,
	FIRST_VALUE(registered_tx_id) OVER (PARTITION BY pool_hash_id, active_partition) AS registered_tx_id
from (
	select 
		ep.epoch_no,
		ep.pool_hash_id,
		phr.pool_id,
		phr.margin as registered_margin,
		phr.pledge as registered_pledge,
		phr.fixed_cost as registered_fixed_cost,
		phr.cert_index as registered_cert_index,
		phr.vrf_key_hash as registered_vrf_key_hash,
		phr.reward_addr_id as registered_reward_addr,
		pha.margin as active_margin,
		pha.pledge as active_pledge,
		pha.fixed_cost as active_fixed_cost,
		pha.cert_index as active_cert_index,
		pha.vrf_key_hash as active_vrf_key_hash,
		pha.reward_addr_id as active_reward_addr,
		pha.registered_tx_id as registered_tx_id,
		case when r.pool_hash_id is not null then 1 else 0 end as has_announced_retirement,
		case when r.retiring_epoch <= ep.epoch_no then 1 else 0 end as is_retired,
		r.retiring_epoch,
		SUM(CASE WHEN phr.margin is not null THEN 1 ELSE 0 END) OVER (PARTITION BY ep.pool_hash_id ORDER BY ep.epoch_no) registered_partition,
		SUM(CASE WHEN pha.margin is not null THEN 1 ELSE 0 END) OVER (PARTITION BY ep.pool_hash_id ORDER BY ep.epoch_no) active_partition
	from epoch_pools ep
		left join v_pool_update phr on ep.epoch_no = phr.epoch_no
			and ep.pool_hash_id = phr.pool_hash_id
			and phr.is_epoch_final = 1
		left join v_pool_update pha on ep.epoch_no = pha.active_epoch_no
			and ep.pool_hash_id = pha.pool_hash_id
			and pha.is_epoch_final = 1
		left join v_pool_retire r on ep.epoch_no >= r.epoch_no
			and r.pool_hash_id = ep.pool_hash_id
			and r.rownum = 1 -- latest record

) a
where is_retired = 0
--order by 1
; 


--------------------------------------------
-- v_pool_owners_by_epoch
-------------------------------------------

drop view if exists v_pool_owners_by_epoch;

create or replace view v_pool_owners_by_epoch
as 
select 
	epoch_no, 
	pe.pool_hash_id, 
	pool_id, 
	po.registered_tx_id, 
	encode(sa.hash_raw, 'hex') as addr, 
	sa.view as bech32_addr,
	sa.id as addr_id
from v_pool_history_by_epoch pe
	inner join pool_owner po on pe.pool_hash_id = po.pool_hash_id
		and pe.registered_tx_id = po.registered_tx_id
	inner join stake_address sa on sa.id = po.addr_id
;



--------------------------------------
-- v_pool_rewards_detail
--------------------------------------
drop view if exists v_pool_rewards_detail;

create or replace view v_pool_rewards_detail
as 
select 
	es.epoch_no,
	r.spendable_epoch as paid_epoch,
	ph.view as pool,
	ph.id as pool_hash_id,
	es.addr_id,
	sa.view as addr,
	es.amount/1000000 as stake_ada,
	r.amount/1000000 as reward_ada, 
	case when es.amount > 0 then (r.amount / es.amount * (365/5)) else 0 end  as individual_roa,
	case when pp.active_reward_addr = es.addr_id then 1 else 0 end as is_rewards_addr,
	case when po.addr_id is not null then 1 else 0 end as is_owner_addr
from epoch_stake es
	left join pool_hash ph on es.pool_id = ph.id
	left join stake_address sa on es.addr_id = sa.id
	left join reward r on es.addr_id = r.addr_id 
							and es.epoch_no = r.earned_epoch
	left join v_pool_history_by_epoch pp on es.epoch_no = pp.epoch_no
		and ph.id = pp.pool_hash_id
	left join v_pool_owners_by_epoch po on es.epoch_no = po.epoch_no
		and ph.id = po.pool_hash_id
		and es.addr_id = po.addr_id
;


--------------------------------------
-- v_pool_rewards_summary
--------------------------------------

drop view if exists v_pool_rewards_summary ;

create view v_pool_rewards_summary
as
select 
	*,
	(pledged_ada * delegator_roa / (365/5))::numeric(16,6) as pledge_rewards,
	total_pool_rewards - (pledged_ada * delegator_roa / (365/5))::numeric(16,6) as operator_rewards
from (
	select 
		epoch_no, 
		pool, 
		pool_hash_id, 
		count(distinct addr_id) as delegators,
		sum(stake_ada)::numeric(16,6)  as stake,
		sum(reward_ada)::numeric(16,6) as rewards, 
		sum(case when is_owner_addr = 1 then stake_ada else null end)::numeric(16,6) as pledged_ada,
		case when sum(stake_ada) > 0 
			then ((sum(reward_ada) / sum(stake_ada)) * (365/5))::numeric(8, 6) else 0 end as overall_roa,
		case when sum(reward_ada) > 0 
			then avg(case when is_rewards_addr = 0 and is_owner_addr = 0 and reward_ada > 0 then individual_roa else null end)::numeric(8,6) else null end as delegator_roa,
		sum(case when is_rewards_addr = 1 then reward_ada else null end)::numeric(16,6) as total_pool_rewards
	from v_pool_rewards_detail pd
	group by 
		epoch_no, 
		pool, 
		pool_hash_id
) a
;

--------------------------------------
-- v_tx_input
--------------------------------------
create or replace view v_tx_input
as
select 
	tx.id as tx_id,
	tx.hash as tx_hash,
	encode(tx.hash, 'hex') as tx_hash_hex,
	tx.fee as tx_fee,
	tx.deposit as tx_deposit,
	tx.size as tx_size,
	b.id as block_id,
	b.epoch_no,
	b.block_no,
	b.slot_no,
	b.epoch_slot_no,
	b.slot_leader_id,
	o.address as input_address,
	o.address_raw as input_address_raw,
	o.index,
	o.value,
	o.stake_address_id,
	o.payment_cred,
	row_number() over (partition by tx_id order by value desc) as val_rank
from tx_out o
	inner join tx_in i on o.tx_id = i.tx_out_id
	inner join tx on tx.id = i.tx_in_id and i.tx_out_index = o.index
	inner join block b on tx.block_id = b.id;



--------------------------------------
-- v_tx_output
--------------------------------------
create or replace view v_tx_output
as
select 
	tx.id as tx_id,
	tx.hash as tx_hash,
	encode(tx.hash, 'hex') as tx_hash_hex,
	tx.fee as tx_fee,
	tx.deposit as tx_deposit,
	tx.size as tx_size,
	b.id as block_id,
	b.epoch_no,
	b.block_no,
	b.slot_no,
	b.epoch_slot_no,
	b.slot_leader_id,
	o.address as output_address,
	o.address_raw as output_address_raw,
	o.index,
	o.value,
	o.stake_address_id,
	o.payment_cred,
	row_number() over (partition by tx_id order by value desc) as val_rank
from tx_out o
	inner join tx on o.tx_id = tx.id
	inner join block b on tx.block_id = b.id;

----------------------------------
-- v_asset_mint
----------------------------------
create or replace view v_asset_mint
as
select 
	encode(ma.policy, 'hex') as policyid,
	encode(ma.name, 'hex') as asset_code,
	convert_from(ma.name, 'utf8') as asset_name,
	b.epoch_no,
	b.time,
	encode(t.hash, 'hex') as tx_hash,
	quantity,
	meta.key as metadata_key,
	meta.json as metadata
from ma_tx_mint m
	inner join tx t on m.tx_id = t.id
	inner join block b on b.id = t.block_id
	inner join multi_asset ma on m.ident = ma.id
	left join tx_metadata meta on m.tx_id = meta.tx_id;


--------------------------------
-- v_oracle_results
--------------------------------

create or replace view v_oracle_results
as
with oracles as (
	select 'NUTS' as oracle_ticker, 'StakeNuts.com' as oracle_name, 'StakeNuts nut.link oracle pool' as oracle_description, 'addr1q85yx2w7ragn5sx6umgmtjpc3865s9sg59sz4rrh6f90kgwfwlzu3w8ttacqg89mkdgwshwnplj5c5n9f8dhp0h55q2q7qm63t' as oracle_address union all
	select 'STKHO', 'STKH Oracle', 'A Cardano Oracle - For the Community, by the Community.', 'addr1v8w6wfzljnzdrwq6patkas35pgjzc3xlggpz70kaldsetcsrw3ep4' union all
	select 'CRFA', 'CardanoFans', 'CardanoFans - we believe in access to financial system for everybody', 'addr1v8yczm692pktwlvjfgwucrullt6af0lme7rh97fhfw2fgjc4chr79' union all
	select 'CANUK', 'Cardano Canucks Oracles', 'A Canadian oracle provider operated by Cardano Canucks Stake Pool', 'addr1qygvjldfxxhp7q96w729c6gvq7hy6pfc937jqlvpms2833rah0c4wey5zfgnuar9eyf6q7pzjzv56c542q7zctpkz9wqay69js'
)

select 
	b.epoch_no,
	b.block_no,
	b.time,
	ora.oracle_ticker,
	m.json,
	jsonb_path_query_first(m.json, '$.ADABTC[*] ? (@.source == "coinGecko")')->>'value' as ADABTC_coingecko,
	jsonb_path_query_first(m.json, '$.ADABTC[*] ? (@.source == "cryptoCompare")')->>'value' as ADABTC_cryptocompare,

	jsonb_path_query_first(m.json, '$.ADAUSD[*] ? (@.source == "coinGecko")')->>'value' as ADAUSD_coingecko,
	jsonb_path_query_first(m.json, '$.ADAUSD[*] ? (@.source == "cryptoCompare")')->>'value' as ADAUSD_cryptocompare,
	jsonb_path_query_first(m.json, '$.ADAUSD[*] ? (@.source == "ergoOracles")')->>'value' as ADAUSD_ergoOracles,

	jsonb_path_query_first(m.json, '$.ADACAD[*] ? (@.source == "coinGecko")')->>'value' as ADACAD_coingecko,
	jsonb_path_query_first(m.json, '$.ADACAD[*] ? (@.source == "cryptoCompare")')->>'value' as ADACAD_cryptocompare,

	jsonb_path_query_first(m.json, '$.ADAEUR[*] ? (@.source == "coinGecko")')->>'value' as ADAEUR_coingecko,
	jsonb_path_query_first(m.json, '$.ADAEUR[*] ? (@.source == "cryptoCompare")')->>'value' as ADAEUR_cryptocompare,

	jsonb_path_query_first(m.json, '$.ADAJPY[*] ? (@.source == "coinGecko")')->>'value' as ADAJPY_coingecko,
	jsonb_path_query_first(m.json, '$.ADAJPY[*] ? (@.source == "cryptoCompare")')->>'value' as ADAJPY_cryptocompare,

	jsonb_path_query_first(m.json, '$.AGIUSD[*] ? (@.source == "coinGecko")')->>'value' as AGIUSD_coingecko,
	jsonb_path_query_first(m.json, '$.AGIUSD[*] ? (@.source == "cryptoCompare")')->>'value' as AGIUSD_cryptocompare,

	jsonb_path_query_first(m.json, '$.BTCUSD[*] ? (@.source == "coinGecko")')->>'value' as BTCUSD_coingecko,
	jsonb_path_query_first(m.json, '$.BTCUSD[*] ? (@.source == "cryptoCompare")')->>'value' as BTCUSD_cryptocompare,

	jsonb_path_query_first(m.json, '$.ERGUSD[*] ? (@.source == "coinGecko")')->>'value' as ERGUSD_coingecko,
	jsonb_path_query_first(m.json, '$.ERGUSD[*] ? (@.source == "ergoOracles")')->>'value' as ERGUSD_ergoOracles,

	jsonb_path_query_first(m.json, '$.TSLA[*] ? (@.source == "investorsExchange")')->>'value' as TSLA_investorsexchange,

	(m.json -> 'DRAND') ->> 'round' as DRAND_round,
	(m.json -> 'DRAND') ->> 'randomness' as DRAND_randomness,
	row_number() over (partition by b.epoch_no, oracle_ticker order by block_no) as epoch_index,
	row_number() over (partition by b.epoch_no, oracle_ticker order by block_no desc) as epoch_index_rev
from tx_out o
	inner join tx_in i on o.tx_id = i.tx_out_id
	inner join tx on tx.id = i.tx_in_id and i.tx_out_index = o.index
	inner join block b on tx.block_id = b.id
	inner join oracles ora on o.address = ora.oracle_address
	inner join tx_metadata m on tx.id = m.tx_id;
