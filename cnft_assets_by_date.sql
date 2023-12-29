    -- This query will give you a snapshot for the provided policyid and date. This will find outputs which were created before the date and are still unspent.
    -- You could use this to find all the tokens in circulation for a given policyid at a given date.
    -- For large policyids this query may take a while to run. You can run this into a temporary table and then query that table for the data you need.
    
    WITH constants (date_utc, policyid) as (
	   values ('2022-04-06 01:43', '\x32f13cdaa51c1ea28221a9f656c498f1d05f7bda49d324d6ff0ad976')
	)
	select 
	    b.block_no,
        b.epoch_no,
        b.time,
        encode(tx.hash, 'hex') as tx_hash,
        o.address, 
        o.stake_address_id,
        o.address_has_script,
        encode(ma.name, 'hex') as asset_code,
        convert_from(ma.name, 'utf8') as token_name, -- depending on the policy, this may error out if there are invalid utf8 chars in the name. You can comment this line out.
        mo.quantity,
        encode(ma.policy, 'hex') as policy,
        sa.view as stake_address
    from tx_out o
		inner join constants c on true
        inner JOIN tx ON tx.id = o.tx_id
        LEFT JOIN block b ON tx.block_id = b.id
        inner join ma_tx_out mo on o.id = mo.tx_out_id
        inner join multi_asset ma on mo.ident = ma.id
        left join stake_address sa on o.stake_address_id = sa.id
        -- associated txs where these outputs are used as inputs - filtered by transaction before our filter date
        LEFT JOIN (
			select i.id, i.tx_in_id, i.tx_out_id, i.tx_out_index, bi.epoch_no, bi.time
			from tx_in i 
				LEFT JOIN tx txi ON txi.id = i.tx_in_id
				LEFT JOIN block bi ON txi.block_id = bi.id
		) i ON o.tx_id = i.tx_out_id AND o.index::smallint = i.tx_out_index::smallint and i.time <= c.date_utc::timestamp
     
    where  i.tx_in_id IS NULL -- where this output is unspent
		and ma.policy = c.policyid::hash28type
		and b.time <= c.date_utc::timestamp -- and the output was created before our filter date
	order by stake_address_id;