from pyspark import SparkContext, SparkConf
import dateutil
from dateutil.parser import parse
import re
from pyspark.sql import Row,SQLContext
# [timestamp,elb,client_port, backend_port, 
#request_processing_time, backend_processing_time, response_processing_time ,
#elb_status_code, backend_status_code, 
#received_bytes, sent_bytes, 
#request, user_agent, 
#ssl_cipher, ssl_protocol] = 


LOG_PATTEN='(\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\d+) (\d+) (\d+) (\d+) "(\S+) (\S+ \S+)" *'
def parse_log(data):
	match = re.search(LOG_PATTEN,data)
	t = parse(match.group(1)).strftime('%s')
	usr_IP,usr_PORT = match.group(3).split(':')
	backend_IP =  match.group(4)
	return Row(
		timestamp    = int(t),
		elb = match.group(2),
		usr_IP = usr_IP,
		usr_PORT=usr_PORT,
		backend_IP = backend_IP,
		action = match.group(12),#get or post action
		URL = match.group(13)
	)

def Sessionize_by_IP(usr_IP,start_t,end_t,sqlContext):
	'''
	Given time window, sessionize the web log by IP.
	'''
	start = "'"+parse(start_t).strftime('%s')+"'"
	end = "'"+parse(end_t).strftime('%s')+"'"
	IP = "'"+usr_IP+"'"
	sql_str = "SELECT usr_IP, URL from logtable where usr_IP = "+IP+' AND timestamp>='+start+' AND timestamp<='+end
	print sql_str
	result = sqlContext.sql(sql_str)
	return result

def unique_IP(sqlContext):
	sql_str = 'select distinct usr_IP from logtable'
	result = sqlContext.sql(sql_str)
	return result
def find_timestamp(sqlContext):
	'''
	sort log records by client IP and timestamp
	'''
	sql_str = "SELECT usr_IP,usr_PORT,backend_IP,URL,timestamp from logtable ORDER BY usr_IP,backend_IP,timestamp"
	result = sqlContext.sql(sql_str)
	return result

def session_time(df):
	'''
	obtain average session length, most engaged user, and IP->(session_length,hits)
	'''
	df_collect = df.collect()
	row = df_collect[0]
	current_usr,current_time,current_backend = row['usr_IP'],row['timestamp'],row['backend_IP']
	last_usr,last_time,last_backend = row['usr_IP'],row['timestamp'],row['backend_IP']
	acitve_threshold = 1800
	max_session_time,max_IP = 0,set()
	URL_counter = set([row['URL']])
	total_session_number,total_session_length = 0.0,0.0
	session_rows=[]
	record_counter,total_recordes = 1,len(df_collect)
	for log in df_collect[1:]:
		URL_counter.add(log['URL'])
		record_counter+=1
		if current_usr==log['usr_IP'] and current_backend == log['backend_IP'] and log['timestamp']-last_time<acitve_threshold:
			last_usr,last_time,last_backend = log['usr_IP'],log['timestamp'],log['backend_IP']
			continue
		else:
			tmp_time = max(last_time-current_time,1) #at least 1 sec
			total_session_number+=1
			total_session_length+= tmp_time
			if max_session_time<tmp_time:
				max_IP = set([log['usr_IP']+':'+log['usr_PORT']])
			elif max_session_time==tmp_time:
				max_IP.add(log['usr_IP']+':'+log['usr_PORT'])
			max_session_time = max(max_session_time,tmp_time)
			tmp_row = Row(
				usr_IP=current_usr,
				backend_IP=current_backend,
				session_length = tmp_time,
				URL_hits = len(URL_counter),
				start_time = current_time
			)
			URL_counter = set()
			session_rows.append(tmp_row)
			current_usr,current_time,current_backend = log['usr_IP'],log['timestamp'],log['backend_IP']
			last_usr,last_time,last_backend = log['usr_IP'],log['timestamp'],log['backend_IP']
		# if record_counter%10000==0:
		# 	print 'finished ', record_counter
	if current_usr==log['usr_IP'] and current_backend == log['backend_IP'] and log['timestamp']-last_time<acitve_threshold:
		tmp_time = max(last_time-current_time,1)
		total_session_number+=1
		total_session_length+= last_time-current_time
		max_session_time = max(max_session_time,tmp_time)
		if max_session_time<tmp_time:
			max_IP = set([log['usr_IP']+':'+log['usr_PORT']])
		elif max_session_time==tmp_time:
			max_IP.add(log['usr_IP']+':'+log['usr_PORT'])
		max_session_time = max(max_session_time,tmp_time)
		tmp_row = Row(
			usr_IP=current_usr,
			backend_IP=current_backend,
			session_length = tmp_time,
			URL_hits = len(URL_counter),
			start_time = current_time
			)		
		session_rows.append(tmp_row)
	print 'max_session time:',max_session_time
	return total_session_length/total_session_number,max_IP,session_rows,max_session_time





