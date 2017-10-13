bag2 = load '/home/franklin/data4pig/000000_0' using PigStorage('\t') as (s_no:long, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray, full_time_position:chararray, prevailing_wage:long, year:chararray, worksite:chararray, longitude:double, latitude:double);

bag1 = load '/home/franklin/data4pig/000001_0' using PigStorage('\t') as (s_no:long, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray, full_time_position:chararray, prevailing_wage:long, year:chararray, worksite:chararray, longitude:double, latitude:double);

bag5 = union bag1, bag2;


bag11 = filter bag5 by year == '2011';
baggrp = group bag11 by case_status;
bagcount = foreach baggrp generate group, COUNT(bag11);

total11 = group bag11 all;
tatal11count = foreach total11 generate COUNT(bag11);

bagpercent = foreach bagcount generate $0, $1, ROUND_TO((double) $1/(double) (tatal11count.$0)*100,2);



bag11 = filter bag5 by year == '2012';
baggrp = group bag11 by case_status;
bagcount = foreach baggrp generate group, COUNT(bag11);

total11 = group bag11 all;
tatal11count = foreach total11 generate COUNT(bag11);

bagpercent12 = foreach bagcount generate $0, $1, ROUND_TO((double) $1/(double) (tatal11count.$0)*100,2);



bag11 = filter bag5 by year == '2013';
baggrp = group bag11 by case_status;
bagcount = foreach baggrp generate group, COUNT(bag11);

total11 = group bag11 all;
tatal11count = foreach total11 generate COUNT(bag11);

bagpercent13 = foreach bagcount generate $0, $1, ROUND_TO((double) $1/(double) (tatal11count.$0)*100,2);




bag11 = filter bag5 by year == '2014';
baggrp = group bag11 by case_status;
bagcount = foreach baggrp generate group, COUNT(bag11);

total11 = group bag11 all;
tatal11count = foreach total11 generate COUNT(bag11);

bagpercent14 = foreach bagcount generate $0, $1, ROUND_TO((double) $1/(double) (tatal11count.$0)*100,2);




bag11 = filter bag5 by year == '2015';
baggrp = group bag11 by case_status;
bagcount = foreach baggrp generate group, COUNT(bag11);

total11 = group bag11 all;
tatal11count = foreach total11 generate COUNT(bag11);

bagpercent15 = foreach bagcount generate $0, $1, ROUND_TO((double) $1/(double) (tatal11count.$0)*100,2);





bag11 = filter bag5 by year == '2016';
baggrp = group bag11 by case_status;
bagcount = foreach baggrp generate group, COUNT(bag11);

total11 = group bag11 all;
tatal11count = foreach total11 generate COUNT(bag11);

bagpercent16 = foreach bagcount generate $0, $1, ROUND_TO((double) $1/(double) (tatal11count.$0)*100,2);



bagjoin = join bagpercent by $0, bagpercent12 by $0, bagpercent13 by $0, bagpercent14 by $0, bagpercent15 by $0, bagpercent16 by $0;

bagjoin = foreach bagjoin generate $0, $1, $2, $4, $5, $7, $8, $10, $11, $13, $14, $16, $17;

dump bagjoin;





