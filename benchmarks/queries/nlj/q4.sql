-- Q4: Small left side with larger right side
select t1.value from range(8192) t1 join range(81920) t2 on t1.value + t2.value < t1.value * t2.value; 