from flask import Flask, Markup, render_template, request
import folium
import pymongo
import datetime
import time
import re
import numpy



circle_colors = ['blue', 'red', 'green', 'purple', 'orange', 'darkred', 'darkblue', 'darkgreen', 'cadetblue',
 'darkpurple', 'white', 'pink', 'lightblue', 'lightgreen', 'gray', 'black', 'lightgray']


def aggregateAll(R):
    keys = {r[0][0] for r in R}
    return [(key, sorted([(int(v), w) for ((k, v), w) in R if k == key])) for key in keys]


def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k, v) in R if k == key])) for key in keys]


def execute():
    ''' calculates the changes over time, and mean change over time of total accidents in revere '''

    client = pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('darren68_gladding_ralcalde', 'darren68_gladding_ralcalde')

    # Get MainStats info
    colName = "darren68_gladding_ralcalde." + "MainStats"
    collection = repo[colName]

    docs = collection.find()
    docList = []

    for doc in docs:
        date = doc['Year']
        numAcc = doc['Accidents']
        oneYearChange = doc['Change From Previous Year']
        PercChange = doc['Percentage Change']

        docList += [(date, numAcc, oneYearChange, PercChange)]


    # Get Severity info
    colName2 = "darren68_gladding_ralcalde." + "SeverityStats"
    collection2 = repo[colName2]

    docs2 = collection2.find()
    docList2 = []

    for doc in docs2:
        date = doc['Year']
        severity = doc['Severity']
        numSevAcc = doc['Accidents']
        change = doc['ChangeFromPrevious']

        docList2 += [(date, severity, numSevAcc, change)]


    repo.logout()
    return [docList, docList2]


def executeAgain():
    ''' calculates the changes over time, and mean change over time of total accidents in revere '''

    client = pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('darren68_gladding_ralcalde', 'darren68_gladding_ralcalde')

    listofCollections = ['darren68_gladding_ralcalde.Clusters2002', 'darren68_gladding_ralcalde.Clusters2003',
                         'darren68_gladding_ralcalde.Clusters2004', 'darren68_gladding_ralcalde.Clusters2005',
                         'darren68_gladding_ralcalde.Clusters2006', 'darren68_gladding_ralcalde.Clusters2007',
                         'darren68_gladding_ralcalde.Clusters2008', 'darren68_gladding_ralcalde.Clusters2009',
                         'darren68_gladding_ralcalde.Clusters2010', 'darren68_gladding_ralcalde.Clusters2011',
                         'darren68_gladding_ralcalde.Clusters2012', 'darren68_gladding_ralcalde.Clusters2013',
                         'darren68_gladding_ralcalde.Clusters2014', 'darren68_gladding_ralcalde.Clusters2015',
                         'darren68_gladding_ralcalde.Clusters2016']

    full_list = []
    for i in range(len(listofCollections)):
        colName = listofCollections[i]
        collection = repo[colName]

        docs = collection.find()
        docList = []

        for doc in docs:
            x = doc['m']['x']
            y = doc['m']['y']
            year = doc["p"]['year']
            docList += [(((x, y), year), 1)]

        full_list += docList

    # sum all tuples with equal cluster and year
    aggregated = aggregate(full_list, sum)

    # aggregate all points into tuples of form: ((locationOfCluster1),[(year2, # accidents), (year2, # accidents)...])
    # where the years are sorted
    appended = aggregateAll(aggregated)
    appendedTop = []

    sums = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    sumsTup = []
    # Number of crashes per cluster per year:
    for i in range(len(appended)):
        for j in range(len(appended[i][1])):
            sums[i] += appended[i][1][j][1]
            # clusterTup.append([(appended[i][0]), appended[i][1][j][1]])
        sumsTup.append([appended[i][0], sums[i]])


    sumsTup.sort(key=lambda x: x[1])


    for i in range(len(appended)):
        if appended[i][0] == sumsTup[-1][0] or appended[i][0] == sumsTup[-2][0] or appended[i][0] == sumsTup[-3][0]:
            appendedTop.append(appended[i])


    # Number of crashes per important cluster per year:
    ImpTup = []
    sumsImp = [0, 0, 0]
    for i in range(len(appendedTop)):
        for j in range(len(appendedTop[i][1])):
            sumsImp[i] += appendedTop[i][1][j][1]
            # clusterTup.append([(appended[i][0]), appended[i][1][j][1]])
        ImpTup.append([appendedTop[i][0], sumsImp[i]])



    # Drop year of important clusters to make y-values vector
    yvalues = []
    cluster_center = []
    for i in range(len(appendedTop)):
        yvalues.append(appendedTop[i][1])
        cluster_center.append(appendedTop[i][0])



    for i in range(len(yvalues)):
        for j in range(len(yvalues[i])):
            yvalues[i][j] = yvalues[i][j][1]


    repo.logout()
    return [cluster_center, yvalues]


results = execute()

cluster_results = executeAgain()


app = Flask(__name__)

# Make bar chart of number of accidents from MainStats
labels = []

j = 0
while j < 15:
    labels.append(results[0][j][0])
    j += 1

b_values = []

i = 0
while i < 15:
    b_values.append(results[0][i][1])
    i += 1

# Make line chart of percentage change of accidents from MainStats
l_values = []

i = 1
while i < 15:
    l_values.append(results[0][i][3])
    i += 1

# Make line chart of number of fatal accidents from SeverityStats
fatal_values = []

i = 0
while i < 15:
    fatal_values.append(results[1][i][2])
    i += 1

# print(fatal_values)

# Make line chart of number of non-fatal accidents from SeverityStats
nonfatal_values = []

i = 15
while i < 30:
    nonfatal_values.append(results[1][i][2])
    i += 1

# print(nonfatal_values)

# Make line chart of number of no-injury accidents from SeverityStats
noinj_values = []

i = 30
while i < 45:
    noinj_values.append(results[1][i][2])
    i += 1

# print(noinj_values)

# Make line chart of number of accidents within each of three clusters
cluster1 = cluster_results[0][0]
cluster2 = cluster_results[0][1]
cluster3 = cluster_results[0][2]

values1 = cluster_results[1][0]
values2 = cluster_results[1][1]
values3 = cluster_results[1][2]



# print(cluster_results)


colors = [
    "#F7464A", "#46BFBD", "#FDB45C", "#FEDCBA",
    "#ABCDEF", "#DDDDDD", "#ABCABC", "#4169E1",
    "#C71585", "#FF4500", "#FEDCBA", "#46BFBD"]


@app.route('/clusters')
def clusters():
    # print(values1)
    c_labels = labels
    c_values = [values1, values2, values3]
    # c_values = values1
    return render_template('index2.html', title='Number of Crashes Within Three Largest Clusters Per Year',
                           labels=c_labels, values=c_values)
    # return render_template('index2.html')


@app.route('/')
def home():
    return render_template('home.html')


@app.route('/numberofcrashes')
def bar():
    # print(b_values)
    bar_labels = labels
    bar_values = b_values
    return render_template('bar_chart.html', title=' Number Crashes Per Year', max=1500, labels=bar_labels,
                           values=bar_values)


@app.route('/percentagechange')
def line():
    # print(l_values)
    line_labels = labels[1:]
    line_values = l_values
    # print(line_values)
    return render_template('line_chart.html', title=' Percentage Change of Number of Crashes Per Year', max=50,
                           labels=line_labels, values=line_values)


@app.route('/fatal')
def fatal():
    # print(b_values)
    f_labels = labels
    f_values = fatal_values
    return render_template('bar_chart.html', title=' Number of Fatal Crashes Per Year', max=10, labels=f_labels,
                           values=f_values)


@app.route('/nonfatal')
def nonfatal():
    # print(b_values)
    nf_labels = labels
    nf_values = nonfatal_values
    return render_template('bar_chart.html', title=' Number of Non-Fatal Crashes Per Year', max=450, labels=nf_labels,
                           values=nf_values)


@app.route('/noinjury')
def noinjury():
    # print(b_values)
    ni_labels = labels
    ni_values = noinj_values
    return render_template('bar_chart.html', title=' Number of Crashes Yielding No Injuries Per Year', max=600,
                           labels=ni_labels, values=ni_values)




@app.route('/maps')
def maps():
    m = folium.Map(location=[42.4063407084367, -71.0034528430666], zoom_start=12)
    m.save('static/folium_test.html')
    return render_template('mappings.html')


@app.route('/display_clusters', methods=['GET', 'POST'])
def display_clusters():
    m = folium.Map(location=[42.4063407084367, -71.0034528430666], zoom_start=12)
    m.save('static/folium_test.html')
    if request.method == 'POST':

        client = pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('darren68_gladding_ralcalde', 'darren68_gladding_ralcalde')


        year = request.form['year']

        tempStr = 'darren68_gladding_ralcalde.Clusters' + year +'LatLng'

        collection = repo[tempStr]


        meanPoints = collection.distinct('m')

        for k in meanPoints:

            try:
                lat = k['lat']
                lng = k['lng']

                folium.Marker(
                    location=[float(lat), float(lng)],
                    icon=folium.Icon(color=circle_colors[meanPoints.index(k)])
                ).add_to(m)
            except:
                continue

        for doc in collection.find():

            try:
                lat = doc['p']['lat']
                lng = doc['p']['lng']

                folium.Circle(
                    radius=10,
                    location=[float(lat), float(lng)],
                    color=circle_colors[meanPoints.index(doc['m'])],
                    fill=True,
                ).add_to(m)
            except:
                continue
        m.save('static/folium_test.html')


    return render_template('cluster_map.html')






@app.route('/display_maps', methods=['GET', 'POST'])
def display_maps():
    m = folium.Map(location=[42.4063407084367, -71.0034528430666], zoom_start=12)
    m.save('static/folium_test.html')
    if request.method == 'POST':

        client = pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('darren68_gladding_ralcalde', 'darren68_gladding_ralcalde')

        collection = repo['darren68_gladding_ralcalde.Revere2001to2016LatLng']

        temp = dict(request.form)


        mannerF = 1
        if temp['manner'][0] == 'no filter':
            mannerF = 0


        try:
            startTime = time.strptime(temp['startTime'][0], "%H:%M")
            endTime = time.strptime(temp['endTime'][0], "%H:%M")
            timeFilter = 1
        except:
            timeFilter = 0
        startDate = datetime.datetime.strptime(temp['startDate'][0], "%Y-%m-%d")
        endDate = datetime.datetime.strptime(temp['endDate'][0], "%Y-%m-%d")


        i = 0
        color = 0
        years = []
        for doc in collection.find({"datetime": {"$gte": startDate, "$lt": endDate}}):
            if i == 0:
                CurrentYear = doc['datetime'].year
                years.append(CurrentYear)
            else:
                if doc['datetime'].year not in years:
                    years.append(doc['datetime'].year)
                    color += 1

            if timeFilter:
                docHour = doc['datetime'].hour
                docMinute = doc['datetime'].minute
                if docHour >= startTime.tm_hour and docHour <= endTime.tm_hour and docMinute >= startTime.tm_min and docMinute <= endTime.tm_min\
                        and ( (mannerF == 1 and doc['mannerofcollision'] == temp['manner'][0]) or (mannerF == 0)):

                    crashNum = doc['crashnumber']
                    Date = doc['datetime'].strftime('%m/%d/%Y')
                    MannerOfC = doc['mannerofcollision']

                    conditions = set()
                    conds = re.findall('\(([^)]+)', doc['drivercontributingcodes'])
                    factors = ''

                    for c in conds:
                        conditions.update([c])
                    for c in conditions:
                        factors += "{}\n".format(c)

                    popUpString = "Crash Number: {}\nDate: {}\nManner of Collision: {}\n" \
                                  "Contributing Factors: {}\n".format(crashNum, Date, MannerOfC, factors)
                    folium.Circle(
                        radius=10,
                        location=[float(doc['latitude']), float(doc['longtitude'])],
                        popup=popUpString,
                        color=circle_colors[color],
                        fill=True,
                    ).add_to(m)
                    #folium.Marker(location=[float(doc['latitude']), float(doc['longtitude'])], popup=popUpString, icon=folium.Icon(color=colors[color])).add_to(m)
            else:
                if (((mannerF == 1 and doc['mannerofcollision'] == temp['manner'][0])) or (mannerF == 0)):
                    crashNum = doc['crashnumber']
                    Date = doc['datetime'].strftime('%m/%d/%Y')
                    MannerOfC = doc['mannerofcollision']

                    conditions = set()
                    conds = re.findall('\(([^)]+)', doc['drivercontributingcodes'])
                    factors = ''

                    for c in conds:
                        conditions.update([c])
                    for c in conditions:
                        factors += "{}\n".format(c)

                    popUpString = "Crash Number: {}\nDate: {}\nManner of Collision: {}\n" \
                                  "Contributing Factors: {}\n".format(crashNum, Date, MannerOfC, factors)
                    #folium.Marker(location=[float(doc['latitude']), float(doc['longtitude'])], popup=popUpString, icon=folium.Icon(color=colors[color])).add_to(m)
                    folium.Circle(
                        radius=10,
                        location=[float(doc['latitude']), float(doc['longtitude'])],
                        popup=popUpString,
                        color=circle_colors[color],
                        fill=True,
                    ).add_to(m)
            i += 1
        m.save('static/folium_test.html')


    return render_template('mappings.html')





if __name__ == "__main__":
    app.debug = True
    app.run()
