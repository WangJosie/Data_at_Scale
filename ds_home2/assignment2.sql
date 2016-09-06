sqlite> .output part_a.txt
sqlite> select count(*) from (select * from db1.Frequency where docid="10398_txt_earn") x;


sqlite> .output part_b.txt
sqlite> . select count(*) from  db1.Frequency where docid="10398_txt_earn" and count =1;

sqlite> .output part_c.txt
sqlite> select count(*) from ( select * from db1.Frequency where docid="10398_txt_earn" and count =1  union select * from db1.Frequency where docid="925_txt_trade " and count=1);

Select count(*) from
(
  SELECT term
  FROM frequency
  WHERE docid = "10398_txt_earn" and [count] = 1

  union

  SELECT term
  FROM frequency
  WHERE docid = "925_txt_trade" and [count] = 1
);


Select distinct count(*) from frequency where term ="legal" or term ="law";




SELECT count(docid) from (
  select docid, sum(count)
  FROM frequency
  group by docid
  having sum(count) > 300
)
;



select count (distinct docid ) from (select docid from frequency where term="transactions" intersect  select docid from frequency where term="world");

#sql for matrix manipulations:

select a.row_num, b.col_mu,, sum(a.value *b.value) from a, b where a.col_num =b.row_num group by a.row_num, b.col_num


#part3, question(H)
#sqlite has to use the the temp view

create temp a as 
select * from frequency where docid="10080_txt_crude" or docid="17035_txt_earn";

create temp b as 
select * frmo a;

select a.docid, b.docid, sum(a.count *b.count)
from a, b
where a.term=b.term
group by a.docid, b.docid




#question key word search
SELECT b.docid, b.term, SUM(a.count * b.count) 
FROM (SELECT * FROM Frequency
      UNION
      SELECT  'q' as docid, 'washington' as term, 1 as count 
      UNION
      SELECT  'q' as docid, 'taxes' as term, 1 as count
      UNION 
      SELECT  'q' as docid, 'treasury' as term, 1 as count 
        ) a, Frequency b
      WHERE a.term = b.term 
      AND a.docid = 'q'
      GROUP BY b.docid, b.term
      ORDER BY SUM(a.count * b.count);



