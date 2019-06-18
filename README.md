###   mysql 和hive Top n查询
##测试数据

```mysql
#假设下面是商品的流水单，商品的id和售卖的季节，要求出各个商品 在哪个季节是卖的最多的
create table product(id int, season varchar(1));
insert into product values(1,"秋");
insert into product values(1,"夏");
insert into product values(1,"夏");

insert into product values(2,"冬");
insert into product values(2,"冬");
insert into product values(2,"冬");
insert into product values(2,"秋");

```




```mysql> select * from product;
+------+--------+
| id   | season |
+------+--------+
|    1 | 秋     |
|    1 | 夏     |
|    1 | 夏     |
|    2 | 冬     |
|    2 | 冬     |
|    2 | 冬     |
|    2 | 秋     |
+------+--------+
7 rows in set (0.00 sec)

```

选出各个id(商品)在 哪个季节的数量最多 ,这里id做为组
预期

1 夏(1商品在夏天的单最多)

2 冬(2商品在冬天的单最多)



在这个子查询里按id分组，选出  count最多的

```
mysql> select id,season,count(1) as count  from product group by id,season;
+------+--------+-------+
| id   | season | count |
+------+--------+-------+
|    1 | 夏     |     2 |
|    1 | 秋     |     1 |
|    2 | 冬     |     3 |
|    2 | 秋     |     1 |
+------+--------+-------+
4 rows in set (0.00 sec)

```


分组 先计算商品在各个季节的数量

```mysql
create table product2  select id,season,count(1) as count  from product group by id,season;

select * from product2;

```


##分组选topN

###mysql
要选top1,只要同组(id相等),只要比他大的数目是0就行,如果要top2,就比他大的数目<2,就是0个和1个

```mysql
select * from product2 pro where ( select count(1) from product2 where id=pro.id and count>pro.count )=0;


```

```
mysql> select * from product2 pro where ( select count(1) from product2 where id=pro.id and count>pro.count )=0;
+------+--------+-------+
| id   | season | count |
+------+--------+-------+
|    1 | 夏     |     2 |
|    2 | 冬     |     3 |
+------+--------+-------+
2 rows in set (0.00 sec)
```

###hive

hive中可以只用row_number 开窗就行

将mysql中的数据通过sqoop导入hive

```mysql
create table product(id int, season string) row format delimited fields terminated by ','';
```

```
sqoop -import --connect jdbc:mysql://192.168.3.25:3306/test --username root --password 123456 --table product --target-dir  hdfs://ns1/user/hive/warehouse/test.db/product

和mysql一样，先用个表保存各个商品的各个季节的总数
create table product2  as  select id,season,count(1) as count  from product group by id,season;

hive> select * from product2;
OK
1       夏      2
1       秋      1
2       冬      3
2       秋      1
Time taken: 0.134 seconds, Fetched: 4 row(s)
hive>
```
分组，选出count最多的行

```mysql
select  id,season ,count ,row_number() over(partition by id order by count desc ) rank from product2 ;
```

**相当于按id 分组，按count排序,和多一个rank字段（）**，要最多的season，只需要选择出rank是1的就行
```
OK

1       夏      2       1

1       秋      1       2

2       冬      3       1

2       秋      1       2
 select id,season,count from (select  id,season ,count ,row_number() over(partition by id order by count desc ) rank from product2) tmp  where tmp.rank=1;

```
 这里使用的是row_number函数，还有rank()函数，会将值相同的排成rank是相同的



