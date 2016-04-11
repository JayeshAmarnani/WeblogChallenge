REGISTER /usr/lib/pig/piggybank.jar;
REGISTER /usr/lib/pig/datafu.jar;

DEFINE Sessionize datafu.pig.sessions.Sessionize('15m');
DEFINE Enumerate datafu.pig.bags.Enumerate('1');

--Load file into Pig dataset
ds_input = load '2015_07_22_mktplace_shop_web_log_sample.log' USING
org.apache.pig.piggybank.storage.CSVExcelStorage(' ', 'NO_MULTILINE', 'WINDOWS')
 as (timestamp:chararray, elb:chararray, client_port:chararray, backend_port:chararray, request_processing_time:float, backend_processing_time:float, response_processing_time:float, elb_status_code:int, backend_status_code:int, received_bytes:int, sent_bytes:int, request:chararray, user_agent:chararray, ssl_cipher:chararray, ssl_protocol:chararray);

--Create subset of the data, selecting only the required columns and part of data
ds_required = FOREACH ds_input GENERATE timestamp as dt, SUBSTRING(client_port,0,INDEXOF(client_port,':',1)) as ip , request as url;

--Sessionize the data
ds_sessionized = FOREACH (GROUP ds_required BY ip) {
     ds_ordered = ORDER ds_required BY dt ;
     GENERATE FLATTEN(Sessionize(ds_ordered)) AS (dt, ip, url, session_id);
};

ds_group_sessionized = GROUP ds_sessionized BY  session_id;

--Prepare dataset with Start time of session, End time of session, Duration
-----Instead of Max(ds_sessionized.ip) in below query , it can also be written as - FLATTEN(SUBSTRING(BagToString(c.ip),0,INDEXOF(c.ip,"_",1)))

ds_final = FOREACH ds_group_sessionized GENERATE MAX(ds_sessionized.ip) as ipaddress, group, MIN(ds_sessionized.dt), MAX(ds_sessionized.dt), SecondsBetween(ToDate(MAX(ds_sessionized.dt)),ToDate(MIN(ds_sessionized.dt))) as seconds;

STORE ds_final INTO 'Sessionized_dump';

--To find average session time per user/ip
ds_group_ipaddress = group ds_final by ipaddress;
ds_avg_session = FOREACH ds_group_ipaddress GENERATE group, AVG(ds_final.seconds);

STORE ds_avg_session INTO 'Average_Session_Time';

--To find the unique url hits per session
ds_group_session = GROUP ds_sessionized BY session_id;
ds_unique_url = FOREACH ds_group_session {
     ds_dist_url = DISTINCT ds_sessionized.url;
     GENERATE group, COUNT(ds_dist_url) as cnt, MAX(ds_sessionized.ip);
};

STORE ds_unique_url INTO 'Unique_Url_Session';

--To find top 10 users/IPs with longest session time
ds_max_seconds = FOREACH ds_group_ipaddress GENERATE group, MAX(ds_final.seconds) as seconds;
ds_ord_max_seconds = ORDER ds_max_seconds by seconds DESC;

ds_final_top =  limit ds_ord_max_seconds 10;

STORE ds_final_top INTO 'Max_Users_Session';

-----End of code----------------

