bag2 = load '/home/franklin/data4pig/000000_0' using PigStorage('\t') as (s_no:long, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray, full_time_position:chararray, prevailing_wage:long, year:chararray, worksite:chararray, longitude:double, latitude:double);

bag1 = load '/home/franklin/data4pig/000001_0' using PigStorage('\t') as (s_no:long, case_status:chararray, employer_name:chararray, soc_name:chararray, job_title:chararray, full_time_position:chararray, prevailing_wage:long, year:chararray, worksite:chararray, longitude:double, latitude:double);


bagUn = union bag1, bag2;


bag4 = group bagUn by employer_name;

bag5 = foreach bag4 generate group, COUNT(bagUn) as nop;


bagf = filter bag5 by nop >= 1000;


bagCCW = filter bagUn by ((case_status == 'CERTIFIED') OR (case_status == 'CERTIFIED-WITHDRAWN'));

bag6 = group bagCCW by employer_name;

bag7 = foreach bag6 generate group, COUNT(bagCCW);


join1 = join bagf by $0, bag7 by $0;

joinr = foreach join1 generate $0, (double)$1, (double)$3;

joinp = foreach joinr generate $0, $1, $2, ROUND_TO($2/$1*100,2);

joinf = filter joinp by $3 >= 70;

final = foreach joinf generate $0, $3;

dump final;


















