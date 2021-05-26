'''
Table -> Movie watchtime

Start_time End_time
5            15
10			 25
40			 55
27			 45
Calculation should exclude any missed time like movietime started from 5 instead of zero.
'''
with a as(select start_time,end_time,max(end_time) over() max_time,lag(end_time,1,0) over(order by end_time) as end_tm from `gcp-essentials-saket.COVID.netflix` order by start_time asc),
b as (select case when end_tm=0 then (0-start_time) when start_time>end_tm then (end_tm - start_time) else 0 end as diff,max_time from a),
c as (select sum(diff) as diff, max_time from b group by max_time)
select diff + max_time as total_watched from c;