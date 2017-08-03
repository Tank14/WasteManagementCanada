from pyspark import SparkContext
from pyspark import SparkConf

# you can modify this variable to specify the estimated cost from pick up point to another
# in Canadaian Dollars assuming each stop will cost $5
cost_per_stop = 5

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))

sc = SparkContext(conf = conf)

# Format of csv file must be: ID,time,lat,lng,weight_KG,last_pickup,full,empty 
lines = sc.textFile("source/MyData_8000.csv")

# ---------- prepare the data set ----------

# remove header
filterDD = lines.filter(lambda l: not l.startswith("ID"))

# remove empty lines
filterDD = filterDD.filter(lambda x: x is not None).filter(lambda x: x != "")

# extract rows -> ID,time,lat,lng,weight_KG,last_pickup,full,empty
filterDD = filterDD.map(lambda line: line.split(","))

# ---------- start filtering and grouping the data set ----------

# filter rules -> is used to apply the garbage pick up rules
def filter_rules(x):
	#for x[6], 1 means full, 0 means not full
	# - if full add to list for pickup
	if int(x[6]) == 1:
		return True
	#for x[7], 1 means empty, 0 means not empty
	#- if not empty check the next condition
	elif int(x[7]) != 1:
		# if garbage hasn't been picked up for 7 days, add to list for pickup
		if(int(x[5]) >= 7):
			return True
	#If else (i.e. empty, or partially full with less than 7 days since last pick up) do not add to list for pickup
	return False

# map group -> is used to group stops by a general location. These coordinates are then formed as the key for the group.
def map_group(x):
	# provide larger area coordinate to be used for grouping by doing the following:
	# - round the float numbers by hundredth and attach them to the array as new element
	lat = round(float(x[2]),2)
	lng = round(float(x[3]),2)

	# - add new key x[8] = string ( use it later as our key when mapping and reducing) that contains "lat/lng"
        latlng = str(lat) + "/" + str(lng)
	x.append(latlng)

	return x

# apply the filter rules
filterDD = filterDD.filter(filter_rules)

# apply the group mapping
MapRDD = filterDD.map(map_group)

# create a key and value -> coordinates for group = new key
MapRDD = MapRDD.map(lambda x: (x[8], [x]))

#create a RDD with the area key, and a list of all stops that had this area key
# reduce by Key -> "lat/lng", [[List],[List]...]
Groups = MapRDD.reduceByKey(lambda a,b: a+b)


# ------- start analyzing --------

# Count stops -> generates a new list that contains the area and number of stops for each area
def Count_stops(x):
	# returns ("Area", Num_of_stops)
	#Number of stops calculated by counting entries in list of stops
	return (x[0],len(x[1]))

# Apply count_stops
Groups_count = Groups.map(Count_stops)

# GET TOTALS

# total cost for the optimized dataset
total_cost = Groups_count.map(lambda x: x[1]*5 ).reduce(lambda a,b: a+b)

# total number for the optimized dataset
total_num_of_stops = Groups_count.map(lambda x: x[1] ).reduce(lambda a,b: a+b)

# Print total stops and cost for each of the optimized areas
def total_stops_per_area(x):
		area = x[0]
		total_stops = x[1]
		print("\nCount for "+area+": "+str(total_stops))
		print("total cost for "+area+" = $" + str(total_stops*cost_per_stop)) # get total stops and multiple it by the cost for each stop
		
# print the totals for each area
Groups_count.foreach(total_stops_per_area)

# print totals for after optimizing the dataset
print("\n\ntotal cost: $"+str(total_cost))
print("\n\ntotal pickup points: "+str(total_num_of_stops))

# output the javascript heatmap source file
# create a new RDD for the format required by the heatmap
# result of each entry is [lat,lng,intensitiy value]

heatmapRDD = filterDD.map(lambda x: "["+x[2]+","+x[3]+',"1"],')
# save result in result folder
# file name is part-00000
heatmapRDD.saveAsTextFile("result")

sc.stop()

