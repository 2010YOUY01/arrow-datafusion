-- Q1: Small cross product with complex filter condition
select t1.value from range(8192) t1 join range(8192) t2 on t1.value + t2.value < t1.value * t2.value; 