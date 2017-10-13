bag2 = load '/home/franklin/data4pig/000000_0' using PigStorage('\t') as (s_no:long, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray, full_time_position:chararray, prevailing_wage:long, year:chararray, worksite:chararray, longitude:double, latitude:double);

bag1 = load '/home/franklin/data4pig/000001_0' using PigStorage('\t') as (s_no:long, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray, full_time_position:chararray, prevailing_wage:long, year:chararray, worksite:chararray, longitude:double, latitude:double);


bag5 = union bag1, bag2;


bag11 = filter bag5 by year == '2011';
bag12 = filter bag5 by year == '2012';
bag13 = filter bag5 by year == '2013';
bag14 = filter bag5 by year == '2014';
bag15 = filter bag5 by year == '2015';
bag16 = filter bag5 by year == '2016';




bag11g = group bag11 by job_title;
bag11r = foreach bag11g generate group, COUNT(bag11);


bag12g = group bag12 by job_title;
bag12r = foreach bag12g generate group, COUNT(bag12);


bag13g = group bag13 by job_title;
bag13r = foreach bag13g generate group, COUNT(bag13);


bag14g = group bag14 by job_title;
bag14r = foreach bag14g generate group, COUNT(bag14);


bag15g = group bag15 by job_title;
bag15r = foreach bag15g generate group, COUNT(bag15);


bag16g = group bag16 by job_title;
bag16r = foreach bag16g generate group, COUNT(bag16);


join1 = join bag11r by $0, bag12r by $0, bag13r by $0, bag14r by $0, bag15r by $0, bag16r by $0;

join2 = foreach join1 generate $0, (double)$1, (double)$3, (double)$5, (double)$7, (double)$9, (double)$11;


avg1 = foreach join2 generate $0, ROUND_TO(($2-$1)/$1*100,2), ROUND_TO(($3-$2)/$2*100,2), ROUND_TO(($4-$3)/$3*100,2), ROUND_TO(($5-$4)/$4*100,2), ROUND_TO(($6-$5)/$5*100,2);

avgf = foreach avg1 generate $0, ROUND_TO(($1+$2+$3+$4+$5)/5,2);

orderavg = order avgf by $1 desc;

limitavg = limit orderavg 5;

dump limitavg;

















