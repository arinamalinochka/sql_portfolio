-- 1. Calculating monthly revenue per user
with monthly_revenue as (
	select
		date(date_trunc('month', payment_date)) payment_month
		,user_id
		,game_name
		,sum(revenue_amount_usd) total_revenue
	from project.games_payments
	group by 1, 2, 3
)
-- 2. Determining dates needed for metrics and revenue calculation for the previous month
, date_cte as(
	select
		user_id
		,payment_month
		,min(payment_month) over (partition by user_id) first_paid_month
		,date(date_trunc('month', payment_month - interval '1 month' )) prev_cal_month
		,date(date_trunc('month', payment_month + interval '1 month' )) next_cal_month
		,lag(payment_month) over (partition by user_id) prev_paid_month
		,lead(payment_month) over (partition by user_id) next_paid_month
		,lag(total_revenue) over (partition by user_id order by payment_month) prev_month_revenue
		,lead(total_revenue) over (partition by user_id order by payment_month) next_month_revenue
		--MRR
		,sum(total_revenue) mrr
	from monthly_revenue
	group by
		user_id
		,payment_month
		,total_revenue
)
---- 3. Calculating metrics
select
	d.payment_month
	,d.user_id
	,gpu.game_name
	,gpu.language
	,gpu.has_older_device_model
	,gpu.age
	,d.mrr
	-- New MRR
	,case
		when d.payment_month = d.first_paid_month
		then d.mrr
	end new_mrr
	-- Churned MRR
	,case
		when d.next_paid_month is null
		or d.next_paid_month != d.next_cal_month
		then d.mrr
	end churned_mrr
	-- Expansion MRR
	,case
		when d.prev_paid_month = d.prev_cal_month
		and d.mrr > sum(d.prev_month_revenue)
		then d.mrr - sum(d.prev_month_revenue)
	end exp_mrr
	-- Contraction MRR
	,case
		when d.prev_paid_month = d.prev_cal_month
		and d.mrr < sum(d.prev_month_revenue)
		then d.mrr - sum(d.prev_month_revenue)
	end contr_mrr
	--	Paid Users
	,case
		when d.mrr > 0
		then d.user_id
	end paid_user
	-- New Paid Users
	,case
		when d.payment_month = d.first_paid_month
		then d.user_id
	end new_paid_user
	-- Churned Users
	,case
		when d.next_paid_month is null
		or d.next_paid_month != d.next_cal_month
		then d.user_id
	end churned_user
from date_cte d
left join project.games_paid_users gpu on gpu.user_id = d.user_id
	group by
		d.payment_month
		,d.user_id
		,gpu.game_name
		,gpu.language
		,gpu.age
		,gpu.has_older_device_model
		,d.mrr
		,d.next_cal_month
		,d.prev_cal_month
		,d.prev_month_revenue
		,d.next_month_revenue
		,d.first_paid_month
		,d.next_paid_month
		,d.prev_paid_month