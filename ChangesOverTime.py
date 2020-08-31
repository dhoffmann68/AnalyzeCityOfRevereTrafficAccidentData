import dml
import prov.model
import datetime
import uuid
import random


class ChangesOverTime(dml.Algorithm):
    contributor = 'darren68_gladding_ralcalde'
    reads = ["darren68_gladding_ralcalde.Revere2001to2016"]
    writes = ['darren68_gladding_ralcalde.MainStats']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()
        ''' calculates the changes over time, and mean change over time of total accidents in revere '''

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('darren68_gladding_ralcalde', 'darren68_gladding_ralcalde')

        colName = "darren68_gladding_ralcalde." + "Revere2001to2016"
        collection = repo[colName]

        docs = collection.find()

        docList = []
        for doc in docs:
            date = str(doc['datetime'].year)
            docList += [(date, doc)]

        # aggregate accidents per year
        accidentsPerYear = ChangesOverTime.aggregateYears(docList, sum)

        changePerYear = []
        adjustedChangePerYear = []

        # iterate through the accidents per year list and calculate rates of change and % change
        for i in range(len(accidentsPerYear) - 1):
            changePerYear += [accidentsPerYear[i + 1][1] - accidentsPerYear[i][1]]
            adjustedChangePerYear += [((accidentsPerYear[i + 1][1] / accidentsPerYear[i][1]) - 1) * 100]

        dictList = []

        for i in range(len(accidentsPerYear)):
            dic = {}
            dic['Year'] = accidentsPerYear[i][0]
            dic['Accidents'] = accidentsPerYear[i][1]

            if i != 0:
                dic['Change From Previous Year'] = changePerYear[i - 1]
                dic['Percentage Change'] = adjustedChangePerYear[i - 1]
            else:
                dic['Change From Previous Year'] = float('Nan')
                dic['Percentage Change'] = float('Nan')

            dictList.append(dic)

        repo.dropCollection("MainStats")
        repo.createCollection("MainStats")
        repo['darren68_gladding_ralcalde.MainStats'].insert_many(dictList)
        repo['darren68_gladding_ralcalde.MainStats'].metadata({'complete': True})

        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}




    @staticmethod
    def aggregateYears(R, f):
        keys = ['2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014',
                '2015', '2016']
        return [(key, f([1 for (k, v) in R if k == key])) for key in keys]

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
        this_script = doc.agent('alg:darren68_gladding_ralcalde#ChangesOverTime',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # Resource = All Towns Data
        resource = doc.entity('dat:darren68_gladding_ralcalde#Revere2001to2016',
                              {'prov:label': 'Crashes', prov.model.PROV_TYPE: 'ont:DataSet', 'ont:Extension': 'json'})

        # Activity
        get_change = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_change, this_script)
        doc.usage(get_change, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation',
                   'ont:Query': ''
                   }
                  )

        ChangeInAccidents = doc.entity('dat:darren68_gladding_ralcalde#MainStats',
                                       {prov.model.PROV_LABEL: 'Compute Main Stats',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(ChangeInAccidents, this_script)
        doc.wasGeneratedBy(ChangeInAccidents, get_change, endTime)
        doc.wasDerivedFrom(ChangeInAccidents, resource, get_change, get_change, get_change)

        repo.logout()

        return doc


