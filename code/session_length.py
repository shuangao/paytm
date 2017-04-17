from log_analysis_ulti import *
import numpy as np
# from geoip import geolite2# pip install python-geoip-geolite2
import pygeoip
def add_IP_info(session_rows,sqlContext):
	'''
	generate dataframe from containing IP, location , session length and hits information
	'''
	rawdata = pygeoip.GeoIP('GeoLiteCity.dat')
	result_rows = []
	idx = 1
	for r in session_rows.collect():
		IP = r['usr_IP']
		# match = geolite2.lookup(IP)
		data = rawdata.record_by_name(IP)
		if not data:
			print r
			idx+=1
			continue
		else:
			new_row = Row(
				IP = IP,
				Latitude = data['latitude'],
				Longitude = data['longitude'],
				country = data['country_name'],
				city = data['city'] if data['city'] else 'NONE',
				avg_length = r['avg_length'],
				hits = r['hits']
				)
			result_rows.append(new_row)
	session_frame = sqlContext.createDataFrame(result_rows)
	return session_frame



def get_sesssion_info_by_IP():
	'''
	generate IP,location,hits,session_length datasets.
	'''
	conf = SparkConf().setAppName('weblog')
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)
	userRDD = sc.textFile("../data/2015_07_22_mktplace_shop_web_log_sample.log")#2015_07_22_mktplace_shop_web_log_sample
	logs =  (userRDD.map(parse_log).cache())
	logs_frame = sqlContext.createDataFrame(logs)
	sqlContext.registerDataFrameAsTable(logs_frame, "logtable")
	# df2 = Sessionize_by_IP('117.239.195.66','2015-07-22T09:00:28.019143Z','2015-07-22T09:15:28.036251Z ',sqlContext)
	time_frame = find_timestamp(sqlContext)
	average_session_time, most_engaged_usrs,session_rows,max_session_time= session_time(time_frame)
	session_frame = sqlContext.createDataFrame(session_rows)
	sqlContext.registerDataFrameAsTable(session_frame, "session_table")
	sql_str = 'select usr_IP,AVG(session_length) as avg_length,SUM(URL_hits) as hits from session_table GROUP BY usr_IP'
	result = sqlContext.sql(sql_str)
	session_frame = add_IP_info(result,sqlContext)
	sqlContext.registerDataFrameAsTable(session_frame, "session_table")
	session_frame.write.parquet('session_IP/session.parquet')

def IP2Value(IP):
	'''
	encode IP into integer
	'''
	tmp = IP.split('.')
	val = 0
	for strs in tmp:
		val*=256
		val+=int(strs)
	return val

def generate_city_IP_dic(seesion_info_frame):
	'''
	generate dictionaries for searching
	'''
	d,d_IP= {},{}
	for i,session in enumerate(seesion_info_frame.collect()):
		d_IP[session['IP']]=[session['avg_length'],session['hits']]
		if d.has_key(session['city']):
			d[session['city']].append([IP2Value(session['IP']),session['avg_length'],session['hits']])
		else:
			d[session['city']]=[[IP2Value(session['IP']),session['avg_length'],session['hits']]]
	for k in d.keys():
		d[k] = sorted(d[k],key=lambda x:x[0])
	return d,d_IP

def Country_AVG_dic(sqlContext):
	'''
	constructing the dictionary containing the average session length and hits for each country.
	'''
	sql_str1 = 'select country, avg(avg_length) as length, avg(hits) as hits from session_table group by country'
	result = sqlContext.sql(sql_str1)
	country_dic = {}
	for r in result.collect():
		country_dic[r['country']]=[r['length'],r['hits']]
	sql_str2 = 'select avg(avg_length) as length, avg(hits) as hits from session_table'
	result = sqlContext.sql(sql_str2).collect()[0]
	country_dic['avg'] = [result['length'],result['hits']]
	return country_dic


def binary_search(points, target_IP):
	target = IP2Value(target_IP)
	begin,end = 0, len(points)-1
	if target<=points[0][0]:
		return points[0]
	if target<=points[-1][0]:
		return points[-1]
	while begin<end-1:
		half = (begin+end)/2
		if target==points[half][0]:
			return points[half]
		if target>points[half][0]:
			begin = half
		else:
			end = half
	if target - points[begin][0] > points[end][0]-target:
		return points[end]
	return points[begin]



def Predict_SessionLength_Hits(IP,d,d_IP,country_avg_length):
	'''
	Use the session length & hits of thenearest neiboughor as the prediction of the given IP
	if we have the record of the IP, return its session length and hits
	if we can't find/locate the city of the IP, return average session length of the country where the IP is
	if we can't find/locate the country of the IP, return average session length of all IP  
	'''
	if d_IP.has_key(IP):
		return d_IP[IP]
	rawdata = pygeoip.GeoIP('GeoLiteCity.dat')
	data =rawdata.record_by_name(IP)
	if not data:
		return country_avg_length['avg'] 
	if not data['city']:
		if not data['country_name'] or not country_avg_length.has_key(data['country_name']):
			return country_avg_length['avg'] 
		return country_avg_length[data['country_name']]
	if d.has_key(data['city']):
		points = d[data['city']]
		points = np.array(points)
		neighbor = binary_search(points,IP)
		return neighbor[1:]
	else:
		return country_avg_length[data['country_name']]


# get_sesssion_info_by_IP()
conf = SparkConf().setAppName('session_info')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
seesion_info_frame =sqlContext.read.parquet('session_IP/session.parquet')
sqlContext.registerDataFrameAsTable(seesion_info_frame, "session_table")
country_dic = Country_AVG_dic(sqlContext)
dc_city,dc_IP = generate_city_IP_dic(seesion_info_frame)
IP = '13.160.249.195'
[IP_sLength,IP_hit]=Predict_SessionLength_Hits(IP,dc_city,dc_IP,country_dic)
print IP_sLength,IP_hit


