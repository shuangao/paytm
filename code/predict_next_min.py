from pyspark import SparkContext, SparkConf
import dateutil
from dateutil.parser import parse
import re
from pyspark.sql import Row,SQLContext
from log_analysis_ulti import *
import numpy as np
from numpy.matlib import repmat
from sklearn.model_selection import cross_val_score
from sklearn import linear_model
import matplotlib.pyplot as plt
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor

def min_sets_init(current):
	current_min = current['timestamp']
	usr_IP_set=set([current['usr_IP']])
	backend_IP_set=set([current['backend_IP']])
	elb_set=set([current['elb']])
	session_set = set([(current['usr_IP'],current['backend_IP'])])
	post,get = 0,0
	if current['action'] == 'GET':
		get+=1
	else:
		post+=1
	return current_min,usr_IP_set,backend_IP_set,session_set,elb_set,post,get


def Gen_min_data(sqlContext):
	''''
	generate features for every minute.
	the minute feature vector x contains the numbers of: 
	active_usrs, serving_backend, active_sessions, active_loader_balancer,get_actions,post_actions,
	y is the average requests by second
	'''
	time_window = 60
	x,y = [],[]
	sql_str = "SELECT usr_IP,backend_IP,elb,action,URL,timestamp from logtable ORDER BY timestamp"
	result = sqlContext.sql(sql_str)
	result = result.collect()
	current = result[0]
	current_min,usr_IP_set,backend_IP_set,session_set,elb_set,post,get = min_sets_init(current)
	for log in result[1:]:
		if log['timestamp']-current_min<time_window:
			usr_IP_set.add(log['usr_IP'])
			backend_IP_set.add(log['backend_IP'])
			session_set.add((log['usr_IP'],log['backend_IP']))
			elb_set.add(log['elb'])
			if log['action'] == 'GET':
				get+=1
			else:
				post+=1
		else:
			feature = [len(usr_IP_set),len(backend_IP_set),len(session_set),len(elb_set),post,get]
			x.append(feature)
			current_min,usr_IP_set,backend_IP_set,session_set,elb_set,post,get = min_sets_init(log)
	if log['timestamp']-current_min<time_window:
		feature = [len(usr_IP_set),len(backend_IP_set),len(session_set),len(elb_set),post,get]
		x.append(feature)
	for feature in x[1:]:
		y.append((feature[-1]+feature[-2])/60.0)
	return x[:-1],y

def Gen_Every_Min_Data(time_window=10):
	'''
	Generate data to predict next min load
	Use 10 min as the time_window as most session (over 90%) ends in 10 mins
	features: 40 features [au_i,as_i,post_i,get_i]: i=1,...,10;
			  au_i denotes the number of acitve users during the last i minutes
			  as_i denotes the number of acitve sessions during the last i minutes
			  post_i denotes the number of posts during the last i minutes
			  get_i denotes the number of gets during the last i minutes
	target:next min load
	'''
	conf = SparkConf().setAppName('weblog')
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)
	userRDD = sc.textFile("../data/2015_07_22_mktplace_shop_web_log_sample.log")#2015_07_22_mktplace_shop_web_log_sample
	logs =  (userRDD.map(parse_log).cache())
	logs_frame = sqlContext.createDataFrame(logs)
	sqlContext.registerDataFrameAsTable(logs_frame, "logtable")
	x,y = Gen_min_data(sqlContext)
	x,y = np.array(x),np.array(y)
	x,y = Gen_Next_min_Data(x,y,time_window)
	print x.shape,y.shape
	np.save('minutes/features.npy',x)
	np.save('minutes/target.npy',y)

def Gen_Next_min_Data(x,y,time_window):
	'''
	Generate data to predict next min load
	Default use 10 min as the time_window as most session (over 90%) ends in 10 mins
	features: [au_i,as_i,post_i,get_i]: i=1,...,time_window;
			  au_i denotes the number of acitve users during the last i minutes
			  as_i denotes the number of acitve sessions during the last i minutes
			  post_i denotes the number of posts during the last i minutes
			  get_i denotes the number of gets during the last i minutes
	target:next min load
	'''
	features = np.zeros((len(x)-time_window+1,4*time_window))
	target = y[time_window-1:]
	feature = np.zeros((4*time_window,))
	feature[:4] = x[time_window-1,(0,2,4,5)]
	for j in range(1,time_window):
		feature[j*4:(j+1)*4] = x[time_window-1-j,(0,2,4,5)]+feature[(j-1)*4:j*4]
	features[0,:]=feature
	for i in range(1,len(features)):
		minus_feature = x[i-1:i-1+time_window,(0,2,4,5)]
		minus_feature = minus_feature[::-1,:].flatten()
		add_feature = repmat(x[time_window-1+i,(0,2,4,5)],1,time_window).flatten()
		features[i] = features[i-1]+add_feature-minus_feature
	return features,target


Gen_Every_Min_Data(time_window = 5) # 5min time window achieves the best performance
lr = linear_model.LinearRegression( normalize=True)
x = np.load('minutes/features.npy')
y = np.load('minutes/target.npy')
# print y
scores = cross_val_score(lr, x, y, scoring='neg_mean_squared_error',cv=len(x)) #MSE renamed... ,LOO CV
print 'LR:',scores.mean()

lasso = linear_model.Lasso(alpha=5*10e2,max_iter=10000,tol=0.1)
scores = cross_val_score(lasso, x, y, scoring='neg_mean_squared_error',cv=len(x))
print 'Lasso:',scores.mean()


tree = DecisionTreeRegressor(max_depth=4)
scores = cross_val_score(tree, x, y, scoring='neg_mean_squared_error',cv=len(x))
print 'DT:',scores.mean()

forest = RandomForestRegressor(random_state=0, n_estimators=10)
scores = cross_val_score(forest, x, y, scoring='neg_mean_squared_error',cv=len(x))
print 'RF:',scores.mean()