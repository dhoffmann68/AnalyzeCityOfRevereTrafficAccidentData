import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import io
from urllib.request import urlopen


class AllTowns1990to2016(dml.Algorithm):
    contributor = 'darren68_gladding_ralcalde'
    reads = []
    writes = ['darren68_gladding_ralcalde.AllTowns1990to2016']

    @staticmethod
    def execute(trial=False):
        '''Retrieve data set of crashes in all MA towns for the years from 2001 to 2016 and store it in Mongo'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('darren68_gladding_ralcalde', 'darren68_gladding_ralcalde')

        # this is the url of the dataset
        url = 'http://datamechanics.io/data/darren68_gladding_ralcalde/TotalCrashesbyTownandYear1990_2016updated.csv'

        # load the url and read it
        response = urllib.request.urlopen(url)
        file = csv.reader(io.StringIO(response.read().decode('utf-8')), delimiter=',')

        # skip the headers
        next(file, None)
        next(file, None)

        dictList = []
        isNotFirst = False

        # iterate through each row in the file and assign each element to the corresponding field in the dictionary
        for row in file:
            dic = {}
            dic['Town'] = row[0]
            dic['1990'] = row[1]
            dic['1991'] = row[2]
            dic['1992'] = row[3]
            dic['1993'] = row[4]
            dic['1994'] = row[5]
            dic['1995'] = row[6]
            dic['1996'] = row[7]
            dic['1997'] = row[8]
            dic['1998'] = row[9]
            dic['1999'] = row[10]
            dic['2000'] = row[11]
            dic['2001'] = row[12]
            dic['2002'] = row[13]
            dic['2003'] = row[14]
            dic['2004'] = row[15]
            dic['2005'] = row[16]
            dic['2006'] = row[17]
            dic['2007'] = row[18]
            dic['2008'] = row[19]
            dic['2009'] = row[20]
            dic['2010'] = row[21]
            dic['2011'] = row[22]
            dic['2012'] = row[23]
            dic['2013'] = row[24]
            dic['2014'] = row[25]
            dic['2015'] = row[26]
            dic['2016'] = row[27]
            dic['total'] = row[28]
            dic['last change'] = row[29]
            dictList.append(dic)

        repo.dropCollection("AllTowns1990to2016")
        repo.createCollection("AllTowns1990to2016")
        repo['darren68_gladding_ralcalde.AllTowns1990to2016'].insert_many(dictList)
        repo['darren68_gladding_ralcalde.AllTowns1990to2016'].metadata({'complete': True})

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
        this_script = doc.agent('alg:darren68_gladding_ralcalde#AllTowns1990to2016',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # Resource = All Towns Data
        resource = doc.entity('dat:darren68_gladding_ralcalde#TotalCrashesbyTownandYear1990_2016updated.csv',
                              {'prov:label': 'Town Populations', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})

        # Activity
        get_pops = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_pops, this_script)

        doc.usage(get_pops, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )

        pops = doc.entity('dat:darren68_gladding_ralcalde#AllTowns1990to2016',
                          {prov.model.PROV_LABEL: 'Towns Populations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(pops, this_script)
        doc.wasGeneratedBy(pops, get_pops, endTime)
        doc.wasDerivedFrom(pops, resource, get_pops, get_pops, get_pops)

        repo.logout()

        return doc
