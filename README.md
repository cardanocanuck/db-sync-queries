# db-sync-queries
A collection of useful views and queries for the cardano-db-sync Postgres database. 

This was mostly born out of the desire to create the final v_pool_rewards_summary, as we were relying on pooltool.io for these calculations and wanted to be able to use our own calculations and not a third parties.

# Usage
In order to start using the views, simply run the **base_views.sql** script against your db-sync database instance

# Base Views

## v_block 
Simply the block table with the addition of the hash of the pool who created the block

## v_pool_update
An enhanced version of the pool_update table with the addition of epoch, block and rewards address info

## v_pool_retire
An enhanced version of the pool_retire table

## v_pool_history_by_epoch
This view provides a history table of the currently registered and upcoming parameters (fixed fee, margin, pledge etc.) per-epoch. 

It will contain a record per pool, per epoch while a pool is active (not retired)

## v_pool_owners_by_epoch
This view provides details of the owner addresses registered to a pool per-epoch (mostly an internal view to be used for rewrads calculations)

## v_pool_rewards_detail
This view contains details of rewards for each address staked to a pool per-epoch

## v_pool_rewards_summary
This view provides an overview of rewards per pool, per epoch.

This view also includes the calculation of pool rewards vs pledge rewards, since all pledge rewards currently go to the rewards address. Note that this currently sums up the rewards for all pledge addresses (if multiple) into a single column. This calculation uses the average delegator roa for that epoch amd applies that to the pledge addresses, and allocates the remaining rewards to the pool.




 
