import datetime
#from pyspark import SparkContext as sc

orderItems = sc.textFile("/public/retail_db/order_items")
orders = sc.textFile("/public/retail_db/orders")
products = sc.textFile("/public/retail_db/products")



orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
revenuePerOrder = orderItemsMap.reduceByKey(lambda curr, next : curr + next)


ordersCompleteOrClosed = orders.filter(lambda oi: oi.split(',')[3] in ['COMPLETE','CLOSED'])
ordersDate = orders.filter(lambda oi: datetime.datetime.strptime( str(oi.split(',')[1]).date())==datetime.datetime(2013,07,25))

ordersMap = orders.map(lambda oi: (int(oi.split(',')[0]),datetime.datetime.strptime(oi.split(",")[1],"%Y-%m-%d %H-%M-%S.%f")))
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(',')[1]),datetime.datetime.strptime(oi.split(",")[4],"%Y-%m-%d %H-%M-%S.%f"))) # datetime conversion

#ord2 = ord1.map(lambda oi: (int(oi.split(',')[1]),float(oi.split(',')[4]))).reduceByKey(lambda x,y: min(x,y)) #reduceByKey

ordersMapJoin = ordersMap.join(orderItemsMap)

def getTopN(rdd, n):
    for i in rdd.take(n):
        print(i)

def collectAll(rdd):
    for i in rdd.collect():
        print(i)

#get count of order status

revenuePerOrder = orderItems.map(lambda oi: (int(oi.split(',')[1]),oi)).groupByKey() #groupByKey
ordCount = orders.map(lambda oi: (str(oi.split(',')[3]), 1)).countByKey() #countBykey

sortedRevenuePerOrder = revenuePerOrder.map(lambda oi: sorted(oi[1], key=lambda x:float(x.split(',')[4]), reverse=True))
minRevenuePerOrder = revenuePerOrder.map(lambda oi : (int(oi[1].split(",")[1]),float(oi[1].split(",")[4] ))).reduceByKey(lambda x,y: min(x,y))

revenuePerOrderAG = orderItemsMap.aggregateByKey((0.0, 0),lambda x,y: (x[0]+y, x[1]+1), lambda x,y: (x[0]+y[0],x[1]+y[1])) #aggregateByKey

#sortByKey
#sorting the data in products with product price in ascending order
productsMap = products.map(lambda oi: (float(oi.split(",")[4]), oi))
# there is an comma separation in column 3 for product table
productMapFilter = products.filter(lambda p: p.split(',')[4] != "").map(lambda oi: (float(oi.split(",")[4]),oi))
productsSortByKey = productsMap.sortByKey()
productsSortedMap = productsSortByKey.map(lambda oi: oi[1])

#Sort the data by product Category and then by product price in descending 
#productsSortByKey = productsMap.sortByKey(False)
productsFilter = products.filter(lambda p: p.split(',')[4] != "")
productsKeysForSorting = productsFilter.map(lambda oi: ((int(oi.split(",")[1]),float(oi.split(",")[4])), oi) )
#((int(oi.split(",")[1]),float(oi.split(",")[4]),oi)))


#Get top N products with lowest price - Global Ranking - sortbyKey and take
products = sc.textFile("/public/retail_db/products")
productsFilter = products.filter(lambda p: p.split(',')[4] != "")
productsMap = productsFilter.map(lambda oi: (float(oi.split(",")[4]),oi))
productsSortByKey = productsMap.sortByKey()
topTenLowestPrice = productsSortByKey.map(lambda oi: oi[1])
getTopN(topTenLowestPrice,10)
# -- The above query give the top 10 lowest priced products 
# -- to get the top 10 expensive products 
productsSortByKey = productsMap.sortByKey(False)
topTenExpensiveProducts = productsSortByKey.map(lambda oi: oi[1])
getTopN(topTenExpensiveProducts,10)
# We did map -> sortByKey -> map -> take


#Get top N products with lowest price - Global Ranking - top or takeOrdered
products = sc.textFile("/public/retail_db/products")
productsFilter = products.filter(lambda p: p.split(',')[4] != "")
topTenLowestProducts = productsFilter.top(5, key=lambda k: float(k.split(",")[4])) # sorted in descending order
# topTenLowestProduct is a list 
topTenLowestProducts = productsFilter.takeOrdered(5, key=lambda k: float(k.split(",")[4])) #Sort in ascending Order


#Get top N products with lowest price - Global Ranking - groupByKey
products = sc.textFile("/public/retail_db/products")
productsFilter = products.filter(lambda p: p.split(',')[4] != "")
productsMap = productsFilter.map(lambda oi: (int(oi.split(",")[1]),oi))
productsGroupBy = productsMap.groupByKey()

topNproductCategories = productsGroupBy.flatMap(lambda oi: sorted(oi[1], key=lambda k: float(k.split(",")[4]),reverse=True))

#Ranking 
t = productsGroupBy.first()
l = list(t[1])
l = sorted(list(t[1]), key=lambda k: float(k.split(",")[4]), reverse=True)

import itertools as it
l_map = map(lambda k: float(k.split(",")[4]), l)
l_map = sorted(l_map, reverse=True)

topThreePrices = sorted(list(set(l_map)), reverse=True)[:3]
topThreePricedProducts = it.takewhile(lambda k: float(k.split(",")[4]) in topThreePrices, l) # it.takewhile iterates till the lambda function is true. 
# Re Writing the above logic for rdd
# it.takewhile(lambda k: float(k.split(",")[4]) in sorted(list(set(k[1].split(",")[4])))[:3], list(k[1]))
#Method to find the top N prices for each category

def topNPricedProducts(rdd,n):
        l= sorted(list(rdd), key=lambda k: float(k.split(",")[4]), reverse=True)
        topNPrices = sorted(list(set(l)), reverse=True)[:n]
        return it.takewhile(lambda k: float(k.split(",")[4]) in topNPrices, l)

#l_rdd = productsGroupBy.flatMap(lambda k: it.takewhile(lambda oi: float(oi.split(",")[4]) in sorted(list(set(oi.split(",")[4])))[:3], list(k[1])))
l_rdd = productsGroupBy.flatMap(lambda k: topNPricedProducts(k[1],3))
#l_rdd_map = l_rdd.map(lambda oi: (float(oi.split(",")[4], oi))


'''
The set theory in PySpark
'''
# Join 




















