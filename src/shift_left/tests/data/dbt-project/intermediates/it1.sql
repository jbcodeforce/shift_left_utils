insert into  it1 

SELECT  
a,b,c
FROM    s1
left join s2 on s1.id = s2.id;