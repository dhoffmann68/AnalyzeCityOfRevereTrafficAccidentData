import urllib.request
import dml
import prov.model
import datetime
import uuid
import csv
import io

urlEnds = ['Clusters2002.csv','Clusters2003.csv', 'Clusters2004.csv', 'Clusters2005.csv',
           'Clusters2006.csv', 'Clusters2007.csv', 'Clusters2008.csv', 'Clusters2009.csv',
           'Clusters2010.csv', 'Clusters2011.csv', 'Clusters2012.csv', 'Clusters2013.csv',
           'Clusters2014.csv', 'Clusters2015.csv', 'Clusters2016.csv']

collections = ['Clusters2002LatLng','Clusters2003LatLng', 'Clusters2004LatLng', 'Clusters2005LatLng',
           'Clusters2006LatLng', 'Clusters2007LatLng', 'Clusters2008LatLng', 'Clusters2009LatLng',
           'Clusters2010LatLng', 'Clusters2011LatLng', 'Clusters2012LatLng', 'Clusters2013LatLng',
           'Clusters2014LatLng', 'Clusters2015LatLng', 'Clusters2016LatLng']

class ClustersLatLng(dml.Algorithm):
    contributor = 'darren68_gladding_ralcalde'
    reads = []
    writes = ['darren68_gladding_ralcalde.Clusters2002LatLng', 'darren68_gladding_ralcalde.Clusters2003LatLng',
              'darren68_gladding_ralcalde.Clusters2004LatLng', 'darren68_gladding_ralcalde.Clusters2005LatLng',
              'darren68_gladding_ralcalde.Clusters2006LatLng', 'darren68_gladding_ralcalde.Clusters2007LatLng',
              'darren68_gladding_ralcalde.Clusters2008LatLng', 'darren68_gladding_ralcalde.Clusters2009LatLng',
              'darren68_gladding_ralcalde.Clusters2010LatLng', 'darren68_gladding_ralcalde.Clusters2011LatLng',
              'darren68_gladding_ralcalde.Clusters2012LatLng', 'darren68_gladding_ralcalde.Clusters2013LatLng',
              'darren68_gladding_ralcalde.Clusters2014LatLng', 'darren68_gladding_ralcalde.Clusters2015LatLng',
              'darren68_gladding_ralcalde.Clusters2016LatLng']

    @staticmethod
    def execute(trial=False):
        '''Retrieve data set of crashes in Revere for the year 2001 to 2016 and store it in Mongo'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('darren68_gladding_ralcalde', 'darren68_gladding_ralcalde')

        colIndex = 0
        for urlAppend in urlEnds:
            # this is the url of the dataset
            url = 'http://datamechanics.io/data/darren68_gladding_ralcalde/' + urlAppend

            # load the url and read it
            response = urllib.request.urlopen(url)
            file = csv.reader(io.StringIO(response.read().decode('utf-8')), delimiter=',')

            # skip the headers
            next(file, None)

            repo.dropCollection(collections[colIndex])
            repo.createCollection(collections[colIndex])

            # iterate through each row in the file and assign each element to the corresponding field in the dictionary
            colStr = 'darren68_gladding_ralcalde.' + collections[colIndex]
            for row in file:
                dic = {}

                dic['m'] = {'lat': row[0], 'lng': row[1]}
                dic['p'] = {'crashID': row[2], 'lat': row[3], 'lng': row[4], 'year': row[5]}

                repo[colStr].insert(dic)


            repo[colStr].metadata({'complete': True})
            colIndex += 1

        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}



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
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        # doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        # Agent, entity, activity
        this_script = doc.agent('alg:darren68_gladding_ralcalde#ClustersLatLng',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})


        '''2002'''
        # Resource = All Towns Data
        resource2002 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2002.csv',
                              {'prov:label': 'Clusters2002', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        # Activity
        get_Clusters2002 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Clusters2002, this_script)

        doc.usage(get_Clusters2002, resource2002, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        cluster2002 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2002LatLng',
                             {prov.model.PROV_LABEL: 'Clusters2002LatLng', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(cluster2002, this_script)
        doc.wasGeneratedBy(cluster2002, get_Clusters2002, endTime)
        doc.wasDerivedFrom(cluster2002, resource2002, get_Clusters2002, get_Clusters2002, get_Clusters2002)

        '''2003'''
        # Resource = All Towns Data
        resource2003 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2003.csv',
                              {'prov:label': 'Clusters2003', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        # Activity
        get_Clusters2003 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Clusters2003, this_script)

        doc.usage(get_Clusters2003, resource2003, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        cluster2003 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2003LatLng',
                             {prov.model.PROV_LABEL: 'Clusters2003LatLng', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(cluster2003, this_script)
        doc.wasGeneratedBy(cluster2003, get_Clusters2003, endTime)
        doc.wasDerivedFrom(cluster2003, resource2003, get_Clusters2003, get_Clusters2003, get_Clusters2003)

        '''2004'''
        # Resource = All Towns Data
        resource2004 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2004.csv',
                              {'prov:label': 'Clusters2004', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        # Activity
        get_Clusters2004 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Clusters2004, this_script)

        doc.usage(get_Clusters2004, resource2004, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        cluster2004 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2004LatLng',
                             {prov.model.PROV_LABEL: 'Clusters2004LatLng', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(cluster2004, this_script)
        doc.wasGeneratedBy(cluster2004, get_Clusters2004, endTime)
        doc.wasDerivedFrom(cluster2004, resource2004, get_Clusters2004, get_Clusters2004, get_Clusters2004)

        '''2005'''
        # Resource = All Towns Data
        resource2005 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2005.csv',
                              {'prov:label': 'Clusters2005', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        # Activity
        get_Clusters2005 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Clusters2005, this_script)

        doc.usage(get_Clusters2005, resource2005, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        cluster2005 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2005LatLng',
                             {prov.model.PROV_LABEL: 'Clusters2005LatLng', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(cluster2005, this_script)
        doc.wasGeneratedBy(cluster2005, get_Clusters2005, endTime)
        doc.wasDerivedFrom(cluster2005, resource2005, get_Clusters2005, get_Clusters2005, get_Clusters2005)

        '''2006'''
        # Resource = All Towns Data
        resource2006 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2006.csv',
                              {'prov:label': 'Clusters2006', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        # Activity
        get_Clusters2006 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Clusters2006, this_script)

        doc.usage(get_Clusters2006, resource2006, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        cluster2006 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2006LatLng',
                             {prov.model.PROV_LABEL: 'Clusters2006LatLng', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(cluster2006, this_script)
        doc.wasGeneratedBy(cluster2006, get_Clusters2006, endTime)
        doc.wasDerivedFrom(cluster2006, resource2006, get_Clusters2006, get_Clusters2006, get_Clusters2006)

        '''2007'''
        # Resource = All Towns Data
        resource2007 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2007.csv',
                              {'prov:label': 'Clusters2007', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        # Activity
        get_Clusters2007 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Clusters2007, this_script)

        doc.usage(get_Clusters2007, resource2007, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        cluster2007 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2007LatLng',
                             {prov.model.PROV_LABEL: 'Clusters2007LatLng', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(cluster2007, this_script)
        doc.wasGeneratedBy(cluster2007, get_Clusters2007, endTime)
        doc.wasDerivedFrom(cluster2007, resource2007, get_Clusters2007, get_Clusters2007, get_Clusters2007)

        '''2008'''
        # Resource = All Towns Data
        resource2008 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2008.csv',
                              {'prov:label': 'Clusters2008', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        # Activity
        get_Clusters2008 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Clusters2008, this_script)

        doc.usage(get_Clusters2008, resource2008, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        cluster2008 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2008LatLng',
                             {prov.model.PROV_LABEL: 'Clusters2008LatLng', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(cluster2008, this_script)
        doc.wasGeneratedBy(cluster2008, get_Clusters2008, endTime)
        doc.wasDerivedFrom(cluster2008, resource2008, get_Clusters2008, get_Clusters2008, get_Clusters2008)

        '''2009'''
        # Resource = All Towns Data
        resource2009 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2009.csv',
                              {'prov:label': 'Clusters2009', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        # Activity
        get_Clusters2009 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Clusters2009, this_script)

        doc.usage(get_Clusters2009, resource2009, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        cluster2009 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2009LatLng',
                             {prov.model.PROV_LABEL: 'Clusters2009LatLng', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(cluster2009, this_script)
        doc.wasGeneratedBy(cluster2009, get_Clusters2009, endTime)
        doc.wasDerivedFrom(cluster2009, resource2009, get_Clusters2009, get_Clusters2009, get_Clusters2009)

        '''2010'''
        # Resource = All Towns Data
        resource2010 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2010.csv',
                              {'prov:label': 'Clusters2010', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        # Activity
        get_Clusters2010 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Clusters2010, this_script)

        doc.usage(get_Clusters2010, resource2010, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        cluster2010 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2010LatLng',
                             {prov.model.PROV_LABEL: 'Clusters2010LatLng', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(cluster2010, this_script)
        doc.wasGeneratedBy(cluster2010, get_Clusters2010, endTime)
        doc.wasDerivedFrom(cluster2010, resource2010, get_Clusters2010, get_Clusters2010, get_Clusters2010)

        '''2011'''
        # Resource = All Towns Data
        resource2011 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2011.csv',
                              {'prov:label': 'Clusters2011', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        # Activity
        get_Clusters2011 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Clusters2011, this_script)

        doc.usage(get_Clusters2011, resource2011, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        cluster2011 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2011LatLng',
                             {prov.model.PROV_LABEL: 'Clusters2011LatLng', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(cluster2011, this_script)
        doc.wasGeneratedBy(cluster2011, get_Clusters2011, endTime)
        doc.wasDerivedFrom(cluster2011, resource2011, get_Clusters2011, get_Clusters2011, get_Clusters2011)

        '''2012'''
        # Resource = All Towns Data
        resource2012 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2012.csv',
                              {'prov:label': 'Clusters2012', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        # Activity
        get_Clusters2012 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Clusters2012, this_script)

        doc.usage(get_Clusters2012, resource2012, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        cluster2012 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2012LatLng',
                             {prov.model.PROV_LABEL: 'Clusters2012LatLng', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(cluster2012, this_script)
        doc.wasGeneratedBy(cluster2012, get_Clusters2012, endTime)
        doc.wasDerivedFrom(cluster2012, resource2012, get_Clusters2012, get_Clusters2012, get_Clusters2012)

        '''2013'''
        # Resource = All Towns Data
        resource2013 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2013.csv',
                              {'prov:label': 'Clusters2013', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        # Activity
        get_Clusters2013 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Clusters2013, this_script)

        doc.usage(get_Clusters2013, resource2013, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        cluster2013 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2013LatLng',
                             {prov.model.PROV_LABEL: 'Clusters2013LatLng', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(cluster2013, this_script)
        doc.wasGeneratedBy(cluster2013, get_Clusters2013, endTime)
        doc.wasDerivedFrom(cluster2013, resource2013, get_Clusters2013, get_Clusters2013, get_Clusters2013)

        '''2014'''
        # Resource = All Towns Data
        resource2014 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2014.csv',
                              {'prov:label': 'Clusters2014', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        # Activity
        get_Clusters2014 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Clusters2014, this_script)

        doc.usage(get_Clusters2014, resource2014, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        cluster2014 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2014LatLng',
                             {prov.model.PROV_LABEL: 'Clusters2014LatLng', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(cluster2014, this_script)
        doc.wasGeneratedBy(cluster2014, get_Clusters2014, endTime)
        doc.wasDerivedFrom(cluster2014, resource2014, get_Clusters2014, get_Clusters2014, get_Clusters2014)

        '''2015'''
        # Resource = All Towns Data
        resource2015 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2015.csv',
                              {'prov:label': 'Clusters2015', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        # Activity
        get_Clusters2015 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Clusters2015, this_script)

        doc.usage(get_Clusters2015, resource2015, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        cluster2015 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2015LatLng',
                             {prov.model.PROV_LABEL: 'Clusters2015LatLng', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(cluster2015, this_script)
        doc.wasGeneratedBy(cluster2015, get_Clusters2015, endTime)
        doc.wasDerivedFrom(cluster2015, resource2015, get_Clusters2015, get_Clusters2015, get_Clusters2015)

        '''2016'''
        # Resource = All Towns Data
        resource2016 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2016.csv',
                              {'prov:label': 'Clusters2016', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        # Activity
        get_Clusters2016 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_Clusters2016, this_script)

        doc.usage(get_Clusters2016, resource2016, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )
        cluster2016 = doc.entity('dat:darren68_gladding_ralcalde#Clusters2016LatLng',
                             {prov.model.PROV_LABEL: 'Clusters2016LatLng', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(cluster2016, this_script)
        doc.wasGeneratedBy(cluster2016, get_Clusters2016, endTime)
        doc.wasDerivedFrom(cluster2016, resource2016, get_Clusters2016, get_Clusters2016, get_Clusters2016)


        repo.logout()

        return doc
        
