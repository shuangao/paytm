from log_analysis_ulti import *
import matplotlib.pyplot as plt
import numpy as np

def session_length_statistics(session_frame,longest_seesion):
	'''
	draw the figure to determine the best timewindow for sessions.
	'''
	result = [0]*(longest_seesion+1)
	for row in session_frame.collect():
		result[int(row['session_length'])]+=1
	
	rs2 = [0]*(longest_seesion+1)
	rs2[0]=result[0]
	for i in range(1,len(rs2)):
		rs2[i] = rs2[i-1]+result[i]
	rs2 = np.array(rs2)
	rs2 = rs2*1.0/rs2[-1]
	plt.plot(rs2)
	plt.xlabel('Session Length')
	plt.ylabel('Accumulated Percentage')
	plt.savefig('Session Info.png')
	plt.show()



conf = SparkConf().setAppName('weblog')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
userRDD = sc.textFile("../data/2015_07_22_mktplace_shop_web_log_sample.log")#2015_07_22_mktplace_shop_web_log_sample
logs =  (userRDD.map(parse_log).cache())
logs_frame = sqlContext.createDataFrame(logs)
sqlContext.registerDataFrameAsTable(logs_frame, "logtable")
# df2 = Sessionize_by_IP('117.239.195.66','2015-07-22T09:00:28.019143Z','2015-07-22T09:15:28.036251Z ',sqlContext)
time_frame = find_timestamp(sqlContext)
'''
get the average_session_time,most_engaged_usrs
'''
average_session_time, most_engaged_usrs,session_rows,max_session_time= session_time(time_frame)
print average_session_time,most_engaged_usrs
'''
get the average url clicks by IP
'''
session_frame = sqlContext.createDataFrame(session_rows)
sqlContext.registerDataFrameAsTable(session_frame, "session_table")
sql_str = 'select usr_IP, avg(URL_hits) from session_table group by usr_IP'
avg_hit = sqlContext.sql(sql_str)


