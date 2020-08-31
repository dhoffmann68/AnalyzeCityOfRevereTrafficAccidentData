import dml
import prov.model
import datetime
import uuid
import random



class Clustering(dml.Algorithm):
    contributor = 'darren68_gladding_ralcalde'
    reads = ["darren68_gladding_ralcalde.Revere2001to2016"]
    writes = ['darren68_gladding_ralcalde.2001To2016XYyear', 'darren68_gladding_ralcalde.Clusters2001',
              'darren68_gladding_ralcalde.Clusters2002', 'darren68_gladding_ralcalde.Clusters2003',
              'darren68_gladding_ralcalde.Clusters2004', 'darren68_gladding_ralcalde.Clusters2005',
              'darren68_gladding_ralcalde.Clusters2006', 'darren68_gladding_ralcalde.Clusters2007',
              'darren68_gladding_ralcalde.Clusters2008', 'darren68_gladding_ralcalde.Clusters2009',
              'darren68_gladding_ralcalde.Clusters2010', 'darren68_gladding_ralcalde.Clusters2011',
              'darren68_gladding_ralcalde.Clusters2012', 'darren68_gladding_ralcalde.Clusters2013',
              'darren68_gladding_ralcalde.Clusters2014', 'darren68_gladding_ralcalde.Clusters2015',
              'darren68_gladding_ralcalde.Clusters2016']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        #inserts the data we'll be working with into Mongo
        Clustering.insertDataPointsToMongo(trial)


        #set up the database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('darren68_gladding_ralcalde', 'darren68_gladding_ralcalde')


        M = [(239673.887, 909308.594), (240769.161, 908024.117), (238643.735, 907653.306),
             (241058.902, 906609.534), (241849.966, 905229.039), (243710.339, 909914.636),
             (241925.672, 905511.633), (241923.300, 907701.006), (241882.390, 906556.636),
             (240030.028, 906304.788), (240429.071, 907342.339), (238838.557, 907994.286)]
        M = Clustering.getMeanPoints(M, repo)
        years = ["2001", "2002","2003","2004","2005","2006","2007","2008","2009","2010",
                 "2011","2012","2013","2014","2015", "2016"]

        colName = "darren68_gladding_ralcalde." + "2001To2016XYyear"
        collection = repo[colName]

        for year in years:
            P = []
            for doc in collection.find({"year": year}):
                P.append((doc['x'], doc['y'], doc['year'], doc['crashID']))

            MPD = [(m, p, Clustering.dist(m, p)) for (m, p) in Clustering.product(M, P)]
            PDs = [(p, d) for (m, p, d) in MPD]
            PD = Clustering.aggregate(PDs, min)
            MP = [(m, p) for ((m, p, d), (p2, d2)) in Clustering.product(MPD, PD) if p == p2 and d == d2]


            dicList = []
            for (m, p) in MP:
                dic = { "m" : {"x": m[0],
                               "y": m[1]
                               },
                        "p": {"x": p[0],
                              "y": p[1],
                              "year": p[2],
                              "crashID": p[3]
                              }
                        }
                dicList.append(dic)

            repo.dropCollection("Clusters" + year)
            repo.createCollection("Clusters" + year)
            repo['darren68_gladding_ralcalde.' + 'Clusters' + year].insert_many(dicList)
            repo['darren68_gladding_ralcalde.' + 'Clusters' + year].metadata({'complete': True})



        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}




    @staticmethod
    def getMeanPoints(M, repo):
        colName = "darren68_gladding_ralcalde." + "2001To2016XYyear"
        collection = repo[colName]

        P = []
        for doc in collection.find({"year": "2002"}):
            P.append((doc['x'], doc['y'], doc['year'], doc['crashID']))
        for doc in collection.find({"year": "2005"}):
            P.append((doc['x'], doc['y'], doc['year'], doc['crashID']))
        for doc in collection.find({"year": "2010"}):
            P.append((doc['x'], doc['y'], doc['year'], doc['crashID']))
        for doc in collection.find({"year": "2012"}):
            P.append((doc['x'], doc['y'], doc['year'], doc['crashID']))
        for doc in collection.find({"year": "2016"}):
            P.append((float(doc['x']), float(doc['y']), doc['year'], doc['crashID']))


        i = 0
        OLD = []
        while OLD != M:

            OLD = M
            #computes the distance between the point in M and the point in P
            MPD = [(m, p, Clustering.dist(m, p)) for (m, p) in Clustering.product(M, P)]


            #gets the list of of points and their distance to an M point
            PDs = [(p, d) for (m, p, d) in MPD]


            #assocaites p point with smallest distance
            PD = Clustering.aggregate(PDs, min)


            MP = [(m, p) for ((m, p, d), (p2, d2)) in Clustering.product(MPD, PD) if p == p2 and d == d2]

            MT = Clustering.aggregate(MP, Clustering.plus)

            M1 = [(m, 1) for (m, _) in MP]
            MC = Clustering.aggregate(M1, sum)
            M = [Clustering.scale(t, c) for ((m, t), (m2, c)) in Clustering.product(MT, MC) if m == m2]
            if i == 40:
                break
            i += 1
        return M



    @staticmethod
    def dist(p, q):
        (x1, y1) = p
        (x2, y2, d, CID) = q
        return (x1 - x2) ** 2 + (y1 - y2) ** 2

    @staticmethod
    def plus(args):
        p = [0, 0]
        for (x, y, d, CID) in args:
            p[0] += x
            p[1] += y
        return tuple(p)


    #passed in ((x,y,date), d)
    @staticmethod
    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k, v) in R if k == key])) for key in keys]



    @staticmethod
    def product(R, S):
        return [(t, u) for t in R for u in S]


    @staticmethod
    def scale(p, c):
        (x, y) = p
        return (x / c, y / c)




    @staticmethod
    def insertDataPointsToMongo(trial=False):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('darren68_gladding_ralcalde', 'darren68_gladding_ralcalde')

        colName = "darren68_gladding_ralcalde." + "Revere2001to2016"
        collection = repo[colName]
        docs = collection.find()


        liOfXs = []
        liOfYs = []
        liOfDates = []
        liOfCIDs = []


        if trial == True:
            count = 0
            for doc in docs:
                count += 1
                if len(doc['x']) != 0 and len(doc['y']) != 0 and ((count % 10) == 0):
                    liOfXs.append(float(doc['x']))
                    liOfYs.append(float(doc['y']))
                    liOfDates.append(str(doc['datetime'].year))
                    liOfCIDs.append(doc['crashnumber'])
        else:
            for doc in docs:
                if len(doc['x']) != 0 and len(doc['y']) != 0:
                    liOfXs.append(float(doc['x']))
                    liOfYs.append(float(doc['y']))
                    liOfDates.append(str(doc['datetime'].year))
                    liOfCIDs.append(doc['crashnumber'])

        # All the data points are here, I should save them into mongo
        # the list of all data points
        P = list(zip(liOfXs, liOfYs, liOfDates, liOfCIDs))
        dicList = []

        for p in P:
            tempDic = {"x": p[0],
                       "y": p[1],
                       "year": p[2],
                       "crashID": p[3]
                       }
            dicList.append(tempDic)

        repo.dropCollection("2001To2016XYyear")
        repo.createCollection("2001To2016XYyear")
        repo['darren68_gladding_ralcalde.2001To2016XYyear'].insert_many(dicList)
        repo['darren68_gladding_ralcalde.2001To2016XYyear'].metadata({'complete': True})

        repo.logout()

        return

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):



        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        # doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        # Agent, entity, activity
        this_script = doc.agent('alg:darren68_gladding_ralcalde#Clustering',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # Resource = All Towns Data
        resource = doc.entity('dat:darren68_gladding_ralcalde#Revere2001to2016',
                              {'prov:label': 'Crashes', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        # Activity
        get_clus = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_clus2001 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_clus2002 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_clus2003 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_clus2004 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_clus2005 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_clus2006 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_clus2007 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_clus2008 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_clus2009 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_clus2010 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_clus2011 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_clus2012 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_clus2013 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_clus2014 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_clus2015 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_clus2016 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_clus, this_script)
        doc.wasAssociatedWith(get_clus2001, this_script)
        doc.wasAssociatedWith(get_clus2002, this_script)
        doc.wasAssociatedWith(get_clus2003, this_script)
        doc.wasAssociatedWith(get_clus2004, this_script)
        doc.wasAssociatedWith(get_clus2005, this_script)
        doc.wasAssociatedWith(get_clus2006, this_script)
        doc.wasAssociatedWith(get_clus2007, this_script)
        doc.wasAssociatedWith(get_clus2008, this_script)
        doc.wasAssociatedWith(get_clus2009, this_script)
        doc.wasAssociatedWith(get_clus2010, this_script)
        doc.wasAssociatedWith(get_clus2011, this_script)
        doc.wasAssociatedWith(get_clus2012, this_script)
        doc.wasAssociatedWith(get_clus2013, this_script)
        doc.wasAssociatedWith(get_clus2014, this_script)
        doc.wasAssociatedWith(get_clus2015, this_script)
        doc.wasAssociatedWith(get_clus2016, this_script)

        doc.usage(get_clus, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_clus2001, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_clus2002, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_clus2003, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_clus2004, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_clus2005, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_clus2006, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_clus2007, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_clus2008, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_clus2009, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_clus2010, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_clus2011, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_clus2012, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_clus2013, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_clus2014, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_clus2015, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_clus2016, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        XYyear2001To2016 = doc.entity('dat:darren68_gladding_ralcalde#2001To2016XYyear',
                                      {prov.model.PROV_LABEL: 'Crashes', prov.model.PROV_TYPE: 'ont:DataSet'})
        cluster2001 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2001',
                                 {prov.model.PROV_LABEL: 'Crashes 2001', prov.model.PROV_TYPE: 'ont:DataSet'})
        cluster2002 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2002',
                                 {prov.model.PROV_LABEL: 'Crashes 2002', prov.model.PROV_TYPE: 'ont:DataSet'})
        cluster2003 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2003',
                                 {prov.model.PROV_LABEL: 'Crashes 2003', prov.model.PROV_TYPE: 'ont:DataSet'})
        cluster2004 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2004',
                                 {prov.model.PROV_LABEL: 'Crashes 2004', prov.model.PROV_TYPE: 'ont:DataSet'})
        cluster2005 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2005',
                                 {prov.model.PROV_LABEL: 'Crashes 2005', prov.model.PROV_TYPE: 'ont:DataSet'})
        cluster2006 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2006',
                                 {prov.model.PROV_LABEL: 'Crashes 2006', prov.model.PROV_TYPE: 'ont:DataSet'})
        cluster2007 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2007',
                                 {prov.model.PROV_LABEL: 'Crashes 2007', prov.model.PROV_TYPE: 'ont:DataSet'})
        cluster2008 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2008',
                                 {prov.model.PROV_LABEL: 'Crashes 2008', prov.model.PROV_TYPE: 'ont:DataSet'})
        cluster2009 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2009',
                                 {prov.model.PROV_LABEL: 'Crashes 2009', prov.model.PROV_TYPE: 'ont:DataSet'})
        cluster2010 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2010',
                                 {prov.model.PROV_LABEL: 'Crashes 2010', prov.model.PROV_TYPE: 'ont:DataSet'})
        cluster2011 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2011',
                                 {prov.model.PROV_LABEL: 'Crashes 2011', prov.model.PROV_TYPE: 'ont:DataSet'})
        cluster2012 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2012',
                                 {prov.model.PROV_LABEL: 'Crashes 2012', prov.model.PROV_TYPE: 'ont:DataSet'})
        cluster2013 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2013',
                                 {prov.model.PROV_LABEL: 'Crashes 2013', prov.model.PROV_TYPE: 'ont:DataSet'})
        cluster2014 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2014',
                                 {prov.model.PROV_LABEL: 'Crashes 2014', prov.model.PROV_TYPE: 'ont:DataSet'})
        cluster2015 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2015',
                                 {prov.model.PROV_LABEL: 'Crashes 2015', prov.model.PROV_TYPE: 'ont:DataSet'})
        cluster2016 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2016',
                                 {prov.model.PROV_LABEL: 'Crashes 2016', prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(XYyear2001To2016, this_script)
        doc.wasGeneratedBy(XYyear2001To2016, get_clus, endTime)
        doc.wasDerivedFrom(XYyear2001To2016, resource, get_clus, get_clus, get_clus)

        doc.wasAttributedTo(cluster2001, this_script)
        doc.wasGeneratedBy(cluster2001, get_clus2001, endTime)
        doc.wasDerivedFrom(cluster2001, resource, get_clus2001, get_clus2001, get_clus2001)

        doc.wasAttributedTo(cluster2002, this_script)
        doc.wasGeneratedBy(cluster2002, get_clus2002, endTime)
        doc.wasDerivedFrom(cluster2002, resource, get_clus2002, get_clus2002, get_clus2002)

        doc.wasAttributedTo(cluster2003, this_script)
        doc.wasGeneratedBy(cluster2003, get_clus2003, endTime)
        doc.wasDerivedFrom(cluster2003, resource, get_clus2003, get_clus2003, get_clus2003)

        doc.wasAttributedTo(cluster2004, this_script)
        doc.wasGeneratedBy(cluster2004, get_clus2004, endTime)
        doc.wasDerivedFrom(cluster2004, resource, get_clus2004, get_clus2004, get_clus2004)

        doc.wasAttributedTo(cluster2005, this_script)
        doc.wasGeneratedBy(cluster2005, get_clus2005, endTime)
        doc.wasDerivedFrom(cluster2005, resource, get_clus2005, get_clus2005, get_clus2005)

        doc.wasAttributedTo(cluster2006, this_script)
        doc.wasGeneratedBy(cluster2006, get_clus2006, endTime)
        doc.wasDerivedFrom(cluster2006, resource, get_clus2006, get_clus2006, get_clus2006)

        doc.wasAttributedTo(cluster2007, this_script)
        doc.wasGeneratedBy(cluster2007, get_clus2007, endTime)
        doc.wasDerivedFrom(cluster2007, resource, get_clus2007, get_clus2007, get_clus2007)

        doc.wasAttributedTo(cluster2008, this_script)
        doc.wasGeneratedBy(cluster2008, get_clus2008, endTime)
        doc.wasDerivedFrom(cluster2008, resource, get_clus2008, get_clus2008, get_clus2008)

        doc.wasAttributedTo(cluster2009, this_script)
        doc.wasGeneratedBy(cluster2009, get_clus2009, endTime)
        doc.wasDerivedFrom(cluster2009, resource, get_clus2009, get_clus2009, get_clus2009)

        doc.wasAttributedTo(cluster2010, this_script)
        doc.wasGeneratedBy(cluster2010, get_clus2010, endTime)
        doc.wasDerivedFrom(cluster2010, resource, get_clus2010, get_clus2010, get_clus2010)

        doc.wasAttributedTo(cluster2011, this_script)
        doc.wasGeneratedBy(cluster2011, get_clus2011, endTime)
        doc.wasDerivedFrom(cluster2011, resource, get_clus2011, get_clus2011, get_clus2011)

        doc.wasAttributedTo(cluster2012, this_script)
        doc.wasGeneratedBy(cluster2012, get_clus2012, endTime)
        doc.wasDerivedFrom(cluster2012, resource, get_clus2012, get_clus2012, get_clus2012)

        doc.wasAttributedTo(cluster2013, this_script)
        doc.wasGeneratedBy(cluster2013, get_clus2013, endTime)
        doc.wasDerivedFrom(cluster2013, resource, get_clus2013, get_clus2013, get_clus2013)

        doc.wasAttributedTo(cluster2014, this_script)
        doc.wasGeneratedBy(cluster2014, get_clus2014, endTime)
        doc.wasDerivedFrom(cluster2014, resource, get_clus2014, get_clus2014, get_clus2014)

        doc.wasAttributedTo(cluster2015, this_script)
        doc.wasGeneratedBy(cluster2015, get_clus2015, endTime)
        doc.wasDerivedFrom(cluster2015, resource, get_clus2015, get_clus2015, get_clus2015)

        doc.wasAttributedTo(cluster2016, this_script)
        doc.wasGeneratedBy(cluster2016, get_clus2016, endTime)
        doc.wasDerivedFrom(cluster2016, resource, get_clus2016, get_clus2016, get_clus2016)


        return doc


