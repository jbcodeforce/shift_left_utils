insert into  int_p5_a 

SELECT  
s1.a,s1.b,s2.c
FROM    src_s1 as s1
left join src_s2 as s2 on s1.id = s2.id;