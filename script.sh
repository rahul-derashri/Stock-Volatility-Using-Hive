echo "************Create database temp**************"
$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS temp;"
$HIVE_HOME/bin/hive -e "create table temp (values STRING);"

echo "************Load data************"
$HIVE_HOME/bin/hive -e "LOAD DATA LOCAL INPATH '$1/*.csv' OVERWRITE INTO TABLE temp;"


echo "************create table stocks************"
$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS stocks;"

echo "************inserting into stocks************"
$HIVE_HOME/bin/hive -e "CREATE TABLE stocks AS
                        SELECT
                        INPUT__FILE__NAME AS Filename,
                        regexp_extract(values, '^(?:([^,]*)\,?){1}', 1) As Date,
                        regexp_extract(values, '^(?:([^,]*)\,?){7}', 1) As Adj_Close
                        FROM temp;"

$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS newstocks;"

echo "************inserting into newstocks************"
$HIVE_HOME/bin/hive -e "CREATE TABLE newstocks AS
                        SELECT 
                        Filename as Filename, 
                        split(Date,'[-]')[0] as Year, 
                        split(Date,'[-]')[1] as Month, 
                        concat_ws('-',split(Date,'[-]')[2],Adj_Close) as Data 
                        FROM stocks;"


#$HIVE_HOME/bin/hive -e "set hive.auto.convert.join.noconditionaltask = true;"
#$HIVE_HOME/bin/hive -e "set hive.auto.convert.join.noconditionaltask.size = 10000;"

echo "************inserting into MAXMINDATA ************"
# Steps for getting Max data
$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS MAXMINDATA;"
$HIVE_HOME/bin/hive -e "CREATE TABLE MAXMINDATA AS
                        SELECT Filename as Filename, 
                        Year as Year, 
                        Month as Month, 
                        MAX(Data) as Max,
                        MIN(Data) as Min 
                        FROM newstocks 
                        GROUP BY Filename, Year, Month;"

echo "******************Dropping unwanted tables********************"
$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS temp;"
$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS stocks;"
$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS newstocks;"


echo "************Calculate RateOfReturn ********************"
$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS RATE;"

$HIVE_HOME/bin/hive -e "create table RATE AS
                        SELECT a.Filename as Filename, 
                        (split(Max,'[-]')[1] - split(Min,'[-]')[1])/split(Min,'[-]')[1] as RATE 
                        from MAXMINDATA a;"

$HIVE_HOME/bin/hive -e "SELECT COUNT(*) FROM RATE;"
echo "************Calculate Volatility************"
# Calculate Volatility
$HIVE_HOME/bin/hive -e "DROP TABLE IF EXISTS VOLATILITY;"

$HIVE_HOME/bin/hive -e "CREATE table VOLATILITY AS
                        SELECT a.Filename as Filename, 
                        STDDEV_SAMP(a.RATE) as Volatility 
                        FROM RATE a 
                        GROUP BY a.Filename 
                        HAVING COUNT(*) > 1;"

echo "************Highest 10 volatility stocks************"
$HIVE_HOME/bin/hive -e "select * from VOLATILITY SORT BY Volatility DESC LIMIT 10;"


echo "************Lowest 10 volatility stocks************"
$HIVE_HOME/bin/hive -e "select * from VOLATILITY WHERE Volatility > 0.0 SORT BY Volatility ASC LIMIT 10;"
