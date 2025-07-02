-- Q3: Small cross product with right join
select t1.value from range(8192) t1 right join range(8192) t2 on t1.value + t2.value > t1.value * t2.value; 