#Requirement to run the code:

1.Pyspark 2.1.0

2.Sklearn

3.Put data in the /data directory

4.Codes are in /code directory




#Files in /code 
1. log_analysis_Ulti.py: functions to process the log
2. log_process.py: containing the solution for log processing and analysis
3. predict_next_min.py:solution for MLE Q1:
	3.1 Using Linear Regression, Lasso, DecisionTree and RandomForest to predict the next minutes load
	3.2 Features are extracted using Gen_Every_Min_Data(timewindow); each feature vector contains the session information in the previous 'timewindow' minutes.
	3.3 feature representation:[au_i,as_i,post_i,get_i]: i=1,...,time_window;			
		au_i denotes the number of acitve users during the last i minutes
		as_i denotes the number of acitve sessions during the last i minutes
		post_i denotes the number of posts during the last i minutes
		get_i denotes the number of gets during the last i minutes
4. session_length.py: solution to predict the URL hits and session length for given IP.
	4.1 Use the session length & hits of thenearest neiboughor as the prediction of the given IP
	4.2 Strategies:
		if we have the record of the IP, return its session length and hits
		if we can't find/locate the city of the IP, return average session length of the country where the IP is
		if we can't find/locate the country of the IP, return average session length of all IP.

5.GeoLiteCity.dat is a public database used to store the IP-location information. 


