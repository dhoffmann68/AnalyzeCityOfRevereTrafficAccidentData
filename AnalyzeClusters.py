import dml
import prov.model
import datetime
import uuid
import random

class AnalyzeClusters(dml.Algorithm):
    contributor = 'darren68_gladding_ralcalde'
    reads = ['darren68_gladding_ralcalde.Clusters2001',
              'darren68_gladding_ralcalde.Clusters2002', 'darren68_gladding_ralcalde.Clusters2003',
              'darren68_gladding_ralcalde.Clusters2004', 'darren68_gladding_ralcalde.Clusters2005',
              'darren68_gladding_ralcalde.Clusters2006', 'darren68_gladding_ralcalde.Clusters2007',
              'darren68_gladding_ralcalde.Clusters2008', 'darren68_gladding_ralcalde.Clusters2009',
              'darren68_gladding_ralcalde.Clusters2010', 'darren68_gladding_ralcalde.Clusters2011',
              'darren68_gladding_ralcalde.Clusters2012', 'darren68_gladding_ralcalde.Clusters2013',
              'darren68_gladding_ralcalde.Clusters2014', 'darren68_gladding_ralcalde.Clusters2015',
              'darren68_gladding_ralcalde.Clusters2016']
    writes = ['darren68_gladding_ralcalde.ClusterEvolution']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()
        '''
        analyze the clusters to get the rate of change of each cluster over the years from 2002 to 2016
        '''
        client = dml.pymongo.MongoClient()
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
        aggregated = AnalyzeClusters.aggregate(full_list, sum)

        # aggregate all points into tuples of form: ((locationOfCluster1),[(year2, # accidents), (year2, # accidents)...])
        # where the years are sorted
        appended = AnalyzeClusters.aggregateAll(aggregated)

        # compute changes for all years after 2002
        for i in range(len(appended)):
            if len(appended[i][1]) < 15:
                appended[i] = AnalyzeClusters.completeYears(appended[i])
            for j in range(len(appended[i][1]) - 1, 0, -1):
                appended[i][1][j] = (appended[i][1][j][0], ((appended[i][1][j][1] - appended[i][1][j - 1][1])))

        dictList = []
        for i in range(len(appended)):
            dic = {}
            dic['Location'] = appended[i][0]
            dic['2002-First Year'] = appended[0][1][0][1]
            dic['2003'] = appended[i][1][1][1]
            dic['2004'] = appended[i][1][2][1]
            dic['2005'] = appended[i][1][3][1]
            dic['2006'] = appended[i][1][4][1]
            dic['2007'] = appended[i][1][5][1]
            dic['2008'] = appended[i][1][6][1]
            dic['2009'] = appended[i][1][7][1]
            dic['2010'] = appended[i][1][8][1]
            dic['2011'] = appended[i][1][9][1]
            dic['2012'] = appended[i][1][10][1]
            dic['2013'] = appended[i][1][11][1]
            dic['2014'] = appended[i][1][12][1]
            dic['2015'] = appended[i][1][13][1]
            dic['2016'] = appended[i][1][14][1]

            dictList.append(dic)

        repo.dropCollection("ClusterEvolution")
        repo.createCollection("ClusterEvolution")
        repo['darren68_gladding_ralcalde.ClusterEvolution'].insert_many(dictList)
        repo['darren68_gladding_ralcalde.ClusterEvolution'].metadata({'complete': True})

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}





    @staticmethod
    def completeYears(tup):
        (mean, lst) = tup
        yearsIncluded = [v for (v, num) in lst]
        yearsNeeded = [i for i in range(2002, 2017)]

        yearsMissing = AnalyzeClusters.difference(yearsNeeded, yearsIncluded)

        for i in yearsMissing:
            lst = lst[:i - 2002] + [(i, 0.0)] + lst[i - 2002:]

        return (mean, lst)

    @staticmethod
    def difference(R, S):
        return [t for t in R if t not in S]

    @staticmethod
    def aggregateAll(R):
        keys = {r[0][0] for r in R}
        return [(key, sorted([(int(v), w) for ((k, v), w) in R if k == key])) for key in keys]

    @staticmethod
    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k, v) in R if k == key])) for key in keys]


    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
                    Create the provenance document describing everything happening
                    in this script. Each run of the script will generate a new
                    document describing that invocation event.
                    '''
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('darren68_gladding_ralcalde', 'darren68_gladding_ralcalde')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Computation', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        # doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        # Agent, entity, activity
        this_script = doc.agent('alg:darren68_gladding_ralcalde#AnalyzeClusters',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # Resource
        resource2001 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2001',
                                  {'prov:label': 'Clustering for 2001', prov.model.PROV_TYPE: 'ont:DataSet',
                                   'ont:Extension': 'json'})
        resource2002 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2002',
                                  {'prov:label': 'Clustering for 2002', prov.model.PROV_TYPE: 'ont:DataSet',
                                   'ont:Extension': 'json'})
        resource2003 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2003',
                                  {'prov:label': 'Clustering for 2003', prov.model.PROV_TYPE: 'ont:DataSet',
                                   'ont:Extension': 'json'})
        resource2004 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2004',
                                  {'prov:label': 'Clustering for 2004', prov.model.PROV_TYPE: 'ont:DataSet',
                                   'ont:Extension': 'json'})
        resource2005 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2005',
                                  {'prov:label': 'Clustering for 2005', prov.model.PROV_TYPE: 'ont:DataSet',
                                   'ont:Extension': 'json'})
        resource2006 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2006',
                                  {'prov:label': 'Clustering for 2006', prov.model.PROV_TYPE: 'ont:DataSet',
                                   'ont:Extension': 'json'})
        resource2007 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2007',
                                  {'prov:label': 'Clustering for 2007', prov.model.PROV_TYPE: 'ont:DataSet',
                                   'ont:Extension': 'json'})
        resource2008 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2008',
                                  {'prov:label': 'Clustering for 2008', prov.model.PROV_TYPE: 'ont:DataSet',
                                   'ont:Extension': 'json'})
        resource2009 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2009',
                                  {'prov:label': 'Clustering for 2009', prov.model.PROV_TYPE: 'ont:DataSet',
                                   'ont:Extension': 'json'})
        resource2010 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2010',
                                  {'prov:label': 'Clustering for 2010', prov.model.PROV_TYPE: 'ont:DataSet',
                                   'ont:Extension': 'json'})
        resource2011 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2011',
                                  {'prov:label': 'Clustering for 2011', prov.model.PROV_TYPE: 'ont:DataSet',
                                   'ont:Extension': 'json'})
        resource2012 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2012',
                                  {'prov:label': 'Clustering for 2012', prov.model.PROV_TYPE: 'ont:DataSet',
                                   'ont:Extension': 'json'})
        resource2013 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2013',
                                  {'prov:label': 'Clustering for 2013', prov.model.PROV_TYPE: 'ont:DataSet',
                                   'ont:Extension': 'json'})
        resource2014 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2014',
                                  {'prov:label': 'Clustering for 2014', prov.model.PROV_TYPE: 'ont:DataSet',
                                   'ont:Extension': 'json'})
        resource2015 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2015',
                                  {'prov:label': 'Clustering for 2015', prov.model.PROV_TYPE: 'ont:DataSet',
                                   'ont:Extension': 'json'})
        resource2016 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2016',
                                  {'prov:label': 'Clustering for 2016', prov.model.PROV_TYPE: 'ont:DataSet',
                                   'ont:Extension': 'json'})

        # Activity
        get_cluster_evol = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        ClusterEvolution = doc.entity('dat:darren68_gladding_ralcalde#ClusterEvolution',
                                      {prov.model.PROV_LABEL: 'Change in Clusters over Time',
                                       prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAssociatedWith(get_cluster_evol, this_script)
        doc.usage(get_cluster_evol, resource2001, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation',
                   'ont:Query': ''
                   }
                  )

        doc.usage(get_cluster_evol, resource2002, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_cluster_evol, resource2003, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_cluster_evol, resource2004, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_cluster_evol, resource2005, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_cluster_evol, resource2006, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_cluster_evol, resource2007, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_cluster_evol, resource2008, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_cluster_evol, resource2009, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_cluster_evol, resource2010, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_cluster_evol, resource2011, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_cluster_evol, resource2012, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_cluster_evol, resource2013, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_cluster_evol, resource2014, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_cluster_evol, resource2015, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation',
                   'ont:Query': ''
                   }
                  )
        doc.usage(get_cluster_evol, resource2016, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation',
                   'ont:Query': ''
                   }
                  )

        doc.wasAttributedTo(ClusterEvolution, this_script)
        doc.wasGeneratedBy(ClusterEvolution, get_cluster_evol, endTime)

        doc.wasDerivedFrom(ClusterEvolution, resource2001, get_cluster_evol, get_cluster_evol, get_cluster_evol)
        doc.wasDerivedFrom(ClusterEvolution, resource2002, get_cluster_evol, get_cluster_evol, get_cluster_evol)
        doc.wasDerivedFrom(ClusterEvolution, resource2003, get_cluster_evol, get_cluster_evol, get_cluster_evol)
        doc.wasDerivedFrom(ClusterEvolution, resource2004, get_cluster_evol, get_cluster_evol, get_cluster_evol)
        doc.wasDerivedFrom(ClusterEvolution, resource2005, get_cluster_evol, get_cluster_evol, get_cluster_evol)
        doc.wasDerivedFrom(ClusterEvolution, resource2006, get_cluster_evol, get_cluster_evol, get_cluster_evol)
        doc.wasDerivedFrom(ClusterEvolution, resource2007, get_cluster_evol, get_cluster_evol, get_cluster_evol)
        doc.wasDerivedFrom(ClusterEvolution, resource2008, get_cluster_evol, get_cluster_evol, get_cluster_evol)
        doc.wasDerivedFrom(ClusterEvolution, resource2009, get_cluster_evol, get_cluster_evol, get_cluster_evol)
        doc.wasDerivedFrom(ClusterEvolution, resource2010, get_cluster_evol, get_cluster_evol, get_cluster_evol)
        doc.wasDerivedFrom(ClusterEvolution, resource2011, get_cluster_evol, get_cluster_evol, get_cluster_evol)
        doc.wasDerivedFrom(ClusterEvolution, resource2012, get_cluster_evol, get_cluster_evol, get_cluster_evol)
        doc.wasDerivedFrom(ClusterEvolution, resource2013, get_cluster_evol, get_cluster_evol, get_cluster_evol)
        doc.wasDerivedFrom(ClusterEvolution, resource2014, get_cluster_evol, get_cluster_evol, get_cluster_evol)
        doc.wasDerivedFrom(ClusterEvolution, resource2015, get_cluster_evol, get_cluster_evol, get_cluster_evol)
        doc.wasDerivedFrom(ClusterEvolution, resource2016, get_cluster_evol, get_cluster_evol, get_cluster_evol)

        repo.logout()

        return doc
