#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[36m"` #Blue
    NUMBER=`echo "\033[33m"` #yellow
    FGRED=`echo "\033[41m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
    echo -e "${MENU}**********************APP MENU***********************${NORMAL}"
    echo -e "${MENU}**${NUMBER} 1a)${MENU} Is the number of petitions with Data Engineer job title increasing over time? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 1b)${MENU} Top 5 job titles who are having highest avg growth in applications. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 2a)${MENU} Which part of the US has the most Data Engineer jobs for each year? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 2b)${MENU} Top 5 job locations for each year. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 3)${MENU} Which industry(SOC_NAME) has the most number of Data Scientist positions? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 4)${MENU} Top 5 Employers With Most Number of petitions filed. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 5)${MENU}  Find the most popular top 10 job positions for H1B visa applications for each year? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 6)${MENU} Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over the period of time. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 7)${MENU} Create a bar graph to depict the number of applications for each year ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 8)${MENU} Average Prevailing Wage for each job ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 9)${MENU} List of employers with success rate 70% and above.${NORMAL}"
    echo -e "${MENU}**${NUMBER} 10)${MENU} List of jobs with success rate 70% and above. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 11)${MENU} Sending q10 data to MySQL. ${NORMAL}"
    echo -e "${MENU}*********************************************${NORMAL}"
    echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT}enter to exit. ${NORMAL}"
    read opt
}
function option_picked() 
{
    COLOR='\033[01;31m' # bold red
    RESET='\033[00;00m' # normal white
    MESSAGE="$1"  #modified to post the correct option selected
    echo -e "${COLOR}${MESSAGE}${RESET}"
}



clear
show_menu
while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in
        1a) clear;
        option_picked "Is the number of petitions with Data Engineer job title increasing over time?";
	hadoop jar h1bPro.jar h1bpkg.Que1a /niit/h1bpro/datafiles /niit/h1bpro/opfiles/q1aop1
	hdfs dfs -cat /niit/h1bpro/opfiles/q1aop1/p*

	hdfs dfs -rm -r /user/franklin/firstmrop
	hdfs dfs -rm -r /niit/h1bpro/opfiles/q1aop1        
	
	show_menu;
        ;;
	
	1b)
	clear;
	option_picked "Top 5 job titles who are having highest avg growth in applications."
	pig -x local -f /home/franklin/h1bProFiles/h1bq1b.pig
	
	show_menu;
	;;
	
        2a) clear;
        option_picked "Which part of the US has the most Data Engineer jobs for each year?";
        hadoop jar h1bPro.jar h1bpkg.Que2a /niit/h1bpro/datafiles /niit/h1bpro/opfiles/q2aop6
	
	hdfs dfs -rm -r /user/franklin/FirstOutput
	hdfs dfs -rm -r /user/franklin/SecondOutput

	hdfs dfs -cat /niit/h1bpro/opfiles/q2aop6/p*

	hdfs dfs -rm -r /niit/h1bpro/opfiles/q2aop6

        show_menu;
        ;;
          
	2b) clear;
	option_picked "Top 5 job locations for each year."
			echo -e "${MENU}Select One Option From Below To Search by year${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 1)${MENU} 2011 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 2)${MENU} 2012 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 3)${MENU} 2013 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 4)${MENU} 2014 ${NORMAL}"
			echo -e "${MENU}**${NUMBER} 5)${MENU} 2015 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 6)${MENU} 2016 ${NORMAL}"
	read yr	
	case $yr in
		1)
		clear;
		option_picked "2011"
		hadoop jar h1bPro.jar Q2bpkg.Q2bDriver /niit/h1bpro/datafiles /niit/h1bpro/opfiles/q2bop2011m1
		hdfs dfs -cat /niit/h1bpro/opfiles/q2bop2011m1/p*
		
		hdfs dfs -rm -r /user/franklin/q2bop1temp
		hdfs dfs -rm -r /niit/h1bpro/opfiles/q2bop2011m1
	
		;;

		2)
		clear;
		option_picked "2012"
		hadoop jar h1bPro.jar Q2bpkg12.Q2bDriver /niit/h1bpro/datafiles /niit/h1bpro/opfiles/q2bop2012m3
		hdfs dfs -cat /niit/h1bpro/opfiles/q2bop2012m3/p*

		hdfs dfs -rm -r /user/franklin/q2bop1temp
		hdfs dfs -rm -r /niit/h1bpro/opfiles/q2bop2012m3

		;;

		3)
		clear;
		option_picked "2013"
		hadoop jar h1bPro.jar Q2bpkg13.Q2bDriver /niit/h1bpro/datafiles /niit/h1bpro/opfiles/q2bop2013m1
		hdfs dfs -cat /niit/h1bpro/opfiles/q2bop2013m1/p*

		hdfs dfs -rm -r /user/franklin/q2bop1temp
		hdfs dfs -rm -r /niit/h1bpro/opfiles/q2bop2013m1

		;;
		
		4)
		clear;
		option_picked "2014"
		hadoop jar h1bPro.jar Q2bpkg14.Q2bDriver /niit/h1bpro/datafiles /niit/h1bpro/opfiles/q2bop2014m1
		hdfs dfs -cat /niit/h1bpro/opfiles/q2bop2014m1/p*

		hdfs dfs -rm -r /user/franklin/q2bop1temp
		hdfs dfs -rm -r /niit/h1bpro/opfiles/q2bop2014m1

		;;

		5)
		clear;
		option_picked "2015"
		hadoop jar h1bPro.jar Q2bpkg15.Q2bDriver /niit/h1bpro/datafiles /niit/h1bpro/opfiles/q2bop2015m1
		hdfs dfs -cat /niit/h1bpro/opfiles/q2bop2015m1/p*

		hdfs dfs -rm -r /user/franklin/q2bop1temp
		hdfs dfs -rm -r /niit/h1bpro/opfiles/q2bop2015m1

		;;

		6)
		clear;
		option_picked "2016"
		hadoop jar h1bPro.jar Q2bpkg16.Q2bDriver /niit/h1bpro/datafiles /niit/h1bpro/opfiles/q2bop2016m1
		hdfs dfs -cat /niit/h1bpro/opfiles/q2bop2016m1/p*

		hdfs dfs -rm -r /user/franklin/q2bop1temp
		hdfs dfs -rm -r /niit/h1bpro/opfiles/q2bop2016m1
	
		;;
		*) echo "Please Select one among the option[1-6]";;
	esac

	show_menu;
	;;
	
  
        3) clear;
        option_picked "Which industry(SOC_NAME) has the most number of Data Scientist positions?";
         hive -e "select soc_name, count(*) as scount from h1b_final where job_title = 'DATA SCIENTIST' and case_status='CERTIFIED' group by soc_name order by scount desc limit 1";
        show_menu;
        ;;
	
        4) clear;
        option_picked "Top 5 Employers With Most Number of petitions filed";
        		echo -e "${MENU}Select One Option From Below To Search by year${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 1)${MENU} 2011 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 2)${MENU} 2012 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 3)${MENU} 2013 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 4)${MENU} 2014 ${NORMAL}"
			echo -e "${MENU}**${NUMBER} 5)${MENU} 2015 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 6)${MENU} 2016 ${NORMAL}"
        read yr
        echo "You've selected ${yr}"
	   		case $yr in
			
				1) clear;
				echo -e "You have selected year ${MENU} 2011 ${NORMAL}"
				hive -e "select employer_name,count(*) as pcount from h1b_final where year='2011' group by employer_name order by pcount desc limit 5;";
				;;
				
				2) clear;
				echo -e "You have selected year ${MENU} 2012 ${NORMAL}"
				hive -e "select employer_name,count(*) as pcount from h1b_final where year='2012' group by employer_name order by pcount desc limit 5;";
				;;
				
				3) clear;
				echo -e "You have selected year ${MENU} 2013 ${NORMAL}"
				hive -e "select employer_name,count(*) as pcount from h1b_final where year='2013' group by employer_name order by pcount desc limit 5;";
				;;
				
				4) clear;
				echo -e "You have selected year ${MENU} 2014 ${NORMAL}"
				hive -e "select employer_name,count(*) as pcount from h1b_final where year='2014' group by employer_name order by pcount desc limit 5;";
				;;

				5) clear;
				echo -e "You have selected year ${MENU} 2015 ${NORMAL}"
				hive -e "select employer_name,count(*) as pcount from h1b_final where year='2015' group by employer_name order by pcount desc limit 5;";
				;;
				
				6) clear;
				echo -e "You have selected year ${MENU} 2016 ${NORMAL}"
				hive -e "select employer_name,count(*) as pcount from h1b_final where year='2016' group by employer_name order by pcount desc limit 5;";
				;;
				*) echo "Please Select one among the option[1-6]";;
			esac

        show_menu;
        ;;
            
	    5) clear;
        option_picked "Find the most popular top 10 job positions for H1B visa applications for each year?";
        
        echo -e "${MENU}Select One Option From Below ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 1)${MENU} For ALL Ps ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 2)${MENU} For Certified Ps ${NORMAL}"
        
	    read n
	    case $n in
                1)	echo -e "U have selected ${MENU} For ALL Ps${NORMAL}"
                                  	
		    	echo -e "${MENU}Select One Option From Below To Search by year${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 1)${MENU} 2011 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 2)${MENU} 2012 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 3)${MENU} 2013 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 4)${MENU} 2014 ${NORMAL}"
			echo -e "${MENU}**${NUMBER} 5)${MENU} 2015 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 6)${MENU} 2016 ${NORMAL}"

	    		read y
			case $y in
				1) clear;
				echo -e "You have selected year ${MENU} 2011 ${NORMAL}"
				hive -e "select job_title, count(*) as num from h1b_final where year='2011' group by job_title order by num desc limit 10;";
				;;
				
				2) clear;
				echo -e "You have selected year ${MENU} 2012 ${NORMAL}"
				hive -e "select job_title, count(*) as num from h1b_final where year='2012' group by job_title order by num desc limit 10;";
				;;
				
				3) clear;
				echo -e "You have selected year ${MENU} 2013 ${NORMAL}"
				hive -e "select job_title, count(*) as num from h1b_final where year='2013' group by job_title order by num desc limit 10;";
				;;
				
				4) clear;
				echo -e "You have selected year ${MENU} 2014 ${NORMAL}"
				hive -e "select job_title, count(*) as num from h1b_final where year='2014' group by job_title order by num desc limit 10;";
				;;

				5) clear;
				echo -e "You have selected year ${MENU} 2015 ${NORMAL}"
				hive -e "select job_title, count(*) as num from h1b_final where year='2015' group by job_title order by num desc limit 10;";
				;;
				
				6) clear;
				echo -e "You have selected year ${MENU} 2016 ${NORMAL}"
				hive -e "select job_title, count(*) as num from h1b_final where year='2016' group by job_title order by num desc limit 10;";
				;;
				*) echo "Please Select one among the option[1-5]";;
			esac
                    ;;		
                    
                2)	echo -e "U have selected ${MENU} For CERTIFIED Ps${NORMAL}"
                                  	
		    	echo -e "${MENU}Select One Option From Below To Search by year${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 1)${MENU} 2011 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 2)${MENU} 2012 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 3)${MENU} 2013 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 4)${MENU} 2014 ${NORMAL}"
			echo -e "${MENU}**${NUMBER} 5)${MENU} 2015 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 6)${MENU} 2016 ${NORMAL}"

	    		read y
			case $y in
				1) clear;
				echo -e "You have selected year ${MENU} 2011 ${NORMAL}"
				hive -e "select job_title, count(*) as num from h1b_final where year='2011' and case_status='CERTIFIED' group by job_title order by num desc limit 10;";
				;;
				
				2) clear;
				echo -e "You have selected year ${MENU} 2012 ${NORMAL}"
				hive -e "select job_title, count(*) as num from h1b_final where year='2012' and case_status='CERTIFIED' group by job_title order by num desc limit 10;";
				;;
				
				3) clear;
				echo -e "You have selected year ${MENU} 2013 ${NORMAL}"
				hive -e "select job_title, count(*) as num from h1b_final where year='2013' and case_status='CERTIFIED' group by job_title order by num desc limit 10;";
				;;
				
				4) clear;
				echo -e "You have selected year ${MENU} 2014 ${NORMAL}"
				hive -e "select job_title, count(*) as num from h1b_final where year='2014' and case_status='CERTIFIED' group by job_title order by num desc limit 10;";
				;;

				5) clear;
				echo -e "You have selected year ${MENU} 2015 ${NORMAL}"
				hive -e "select job_title, count(*) as num from h1b_final where year='2015' and case_status='CERTIFIED' group by job_title order by num desc limit 10;";
				;;
				
				6) clear;
				echo -e "You have selected year ${MENU} 2016 ${NORMAL}"
				hive -e "select job_title, count(*) as num from h1b_final where year='2016' and case_status='CERTIFIED' group by job_title order by num desc limit 10;";
				;;
				*) echo "Please Select one among the option[1-5]";;
			esac
                    ;;	
                    
                
                    
                *) echo "Please Select one among the option[1-5]";;
                esac
                show_menu;
                    ;;
                    
        6) clear;
        option_picked "Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over the period of time.";
	   
	pig -x local -f h1bProFiles/q6.pig

	xdg-open h1bProFiles/Q6lgraph.ods


        show_menu;
        ;;
        
        7) clear;
        option_picked "Create a bar graph to depict the number of applications for each year";
	
	hadoop jar h1bPro.jar h1bpkg.Que7 /niit/h1bpro/datafiles /niit/h1bpro/opfiles/q7op1
	hdfs dfs -cat /niit/h1bpro/opfiles/q7op1/p*

	hdfs dfs -rm -r /niit/h1bpro/opfiles/q7op1

	xdg-open h1bProFiles/Q7graph.ods

        show_menu;
        ;;
        
	    8) clear;
        option_picked "Average Prevailing Wage for each job";
        echo -e "${MENU}Select One Option From Below ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 1)${MENU} Full Time Jobs ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 2)${MENU} Part Time Jobs ${NORMAL}"
       
	    read job
	    case $job in
		1) clear;
		echo -e "U have selected Full time jobs option"
			echo -e "Select a year"
			echo -e "${MENU}**${NUMBER} 1)${MENU} 2011 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 2)${MENU} 2012 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 3)${MENU} 2013 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 4)${MENU} 2014 ${NORMAL}"
			echo -e "${MENU}**${NUMBER} 5)${MENU} 2015 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 6)${MENU} 2016 ${NORMAL}"
		read year
			case $year in
                		1) clear;
				echo -e "You have selected year ${MENU} 2011 ${NORMAL}"
				hive -e "select job_title, sum(prevailing_wage)/count(*) as avg1 from h1b_final where year='2011' and full_time_position='Y' and case_status in ('CERTIFIED', 'CERTIFIED-WITHDRAWN') group by job_title order by avg1 desc;";
				;;
				
				2) clear;
				echo -e "You have selected year ${MENU} 2012 ${NORMAL}"
				hive -e "select job_title, sum(prevailing_wage)/count(*) as avg1 from h1b_final where year='2012' and full_time_position='Y' and case_status in ('CERTIFIED', 'CERTIFIED-WITHDRAWN') group by job_title order by avg1 desc;";
				;;
				
				3) clear;
				echo -e "You have selected year ${MENU} 2013 ${NORMAL}"
				hive -e "select job_title, sum(prevailing_wage)/count(*) as avg1 from h1b_final where year='2013' and full_time_position='Y' and case_status in ('CERTIFIED', 'CERTIFIED-WITHDRAWN') group by job_title order by avg1 desc;";
				;;
				
				4) clear;
				echo -e "You have selected year ${MENU} 2014 ${NORMAL}"
				hive -e "select job_title, sum(prevailing_wage)/count(*) as avg1 from h1b_final where year='2014' and full_time_position='Y' and case_status in ('CERTIFIED', 'CERTIFIED-WITHDRAWN') group by job_title order by avg1 desc;";
				;;

				5) clear;
				echo -e "You have selected year ${MENU} 2015 ${NORMAL}"
				hive -e "select job_title, sum(prevailing_wage)/count(*) as avg1 from h1b_final where year='2015' and full_time_position='Y' and case_status in ('CERTIFIED', 'CERTIFIED-WITHDRAWN') group by job_title order by avg1 desc;";
				;;
				
				6) clear;
				echo -e "You have selected year ${MENU} 2016 ${NORMAL}"
				hive -e "select job_title, sum(prevailing_wage)/count(*) as avg1 from h1b_final where year='2016' and full_time_position='Y' and case_status in ('CERTIFIED', 'CERTIFIED-WITHDRAWN') group by job_title order by avg1 desc;";
				;;
                    		 *) echo "Please Select one among the option[1-5]";;
                	esac;;
		2) clear;
		echo -e "U have selected Part time jobs option"
			echo -e "Select a year"
			echo -e "${MENU}**${NUMBER} 1)${MENU} 2011 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 2)${MENU} 2012 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 3)${MENU} 2013 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 4)${MENU} 2014 ${NORMAL}"
			echo -e "${MENU}**${NUMBER} 5)${MENU} 2015 ${NORMAL}"
        		echo -e "${MENU}**${NUMBER} 6)${MENU} 2016 ${NORMAL}"
		read year
			case $year in
                		1) clear;
				echo -e "You have selected year ${MENU} 2011 ${NORMAL}"
				hive -e "select job_title, sum(prevailing_wage)/count(*) as avg1 from h1b_final where year='2011' and full_time_position !='Y' and case_status in ('CERTIFIED', 'CERTIFIED-WITHDRAWN') group by job_title order by avg1 desc;";
				;;
				
				2) clear;
				echo -e "You have selected year ${MENU} 2012 ${NORMAL}"
				hive -e "select job_title, sum(prevailing_wage)/count(*) as avg1 from h1b_final where year='2012' and full_time_position !='Y' and case_status in ('CERTIFIED', 'CERTIFIED-WITHDRAWN') group by job_title order by avg1 desc;";
				;;
				
				3) clear;
				echo -e "You have selected year ${MENU} 2013 ${NORMAL}"
				hive -e "select job_title, sum(prevailing_wage)/count(*) as avg1 from h1b_final where year='2013' and full_time_position !='Y' and case_status in ('CERTIFIED', 'CERTIFIED-WITHDRAWN') group by job_title order by avg1 desc;";
				;;
				
				4) clear;
				echo -e "You have selected year ${MENU} 2014 ${NORMAL}"
				hive -e "select job_title, sum(prevailing_wage)/count(*) as avg1 from h1b_final where year='2014' and full_time_position !='Y' and case_status in ('CERTIFIED', 'CERTIFIED-WITHDRAWN') group by job_title order by avg1 desc;";
				;;

				5) clear;
				echo -e "You have selected year ${MENU} 2015 ${NORMAL}"
				hive -e "select job_title, sum(prevailing_wage)/count(*) as avg1 from h1b_final where year='2015' and full_time_position !='Y' and case_status in ('CERTIFIED', 'CERTIFIED-WITHDRAWN') group by job_title order by avg1 desc;";
				;;
				
				6) clear;
				echo -e "You have selected year ${MENU} 2016 ${NORMAL}"
				hive -e "select job_title, sum(prevailing_wage)/count(*) as avg1 from h1b_final where year='2016' and full_time_position !='Y' and case_status in ('CERTIFIED', 'CERTIFIED-WITHDRAWN') group by job_title order by avg1 desc;";
				;;
                    		 *) echo "Please Select one among the option[1-5]";;
                	esac;;
		


                *) echo "Please Select one among the option[1-5]";;
                esac
                show_menu;
                    ;;

	9)
	clear;
	echo -e "Showing list of employers with success rate 70% and above."
	pig -x local -f /home/franklin/h1bProFiles/h1bpro.pig
	
	show_menu;
	;;

	10)
	clear;
	echo -e "Showing list of jobs with success rate 70% and above."
	pig -x mapreduce -f /home/franklin/h1bProFiles/h1bq10.pig
	
	show_menu;
	;;

	11)
	clear;
	echo -e "Sending output of question 10 to MySQL table."
	sqoop export --connect jdbc:mysql://localhost/h1bdb --username root --password 'adam' --table h1bq11 --update-mode allowinsert --update-key job_title --export-dir /q10op3/part-r-00000 --input-fields-terminated-by '\t';
	show_menu;
	;;

        \n) exit;
        ;;

        *) clear;
        option_picked "Pick an option from the menu";
        show_menu;
        ;;
    esac
fi



done


