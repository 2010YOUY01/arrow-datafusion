-- Q5: Very small left side with very large right side (greater than)
select t1.value from range(100) t1 join range(819200) t2 on t1.value + t2.value > t1.value * t2.value; 