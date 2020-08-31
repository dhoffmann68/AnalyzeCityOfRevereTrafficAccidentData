import dml
import prov.model
import datetime
import uuid
import random
import statistics



class AdvancedAnalyzeClusters(dml.Algorithm):
    contributor = 'darren68_gladding_ralcalde'
    reads = ["darren68_gladding_ralcalde.ClusterEvolution"]
    writes = ['darren68_gladding_ralcalde.AdvancedClusterAnalysis']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        '''
        Analyze the rates of change of each cluster and find years that had meaningful impacts on each cluster
        '''

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('darren68_gladding_ralcalde', 'darren68_gladding_ralcalde')

        colName = "darren68_gladding_ralcalde." + "ClusterEvolution"
        collection = repo[colName]

        docs = collection.find()

        dicList = []
        # generate a dictionary of -> the cluster as a Key, The mean change, the Stdrd dev, and the meaningful years
        for doc in docs:
            info = list(doc.values())
            list_of_Changes = info[2:]

            mean = sum(list_of_Changes) / len(list_of_Changes)
            strdDev = statistics.stdev(list_of_Changes)
            meaningfulYears = AdvancedAnalyzeClusters.getMeaningfulYears(list_of_Changes, strdDev)

            dic = {}
            dic['Location of Cluster'] = info[0]
            dic['Mean Change'] = mean
            dic['Stdrd Deviation'] = strdDev
            dic['Meaningful Years'] = meaningfulYears

            dicList.append(dic)

        repo.dropCollection("AdvancedClusterAnalysis")
        repo.createCollection("AdvancedClusterAnalysis")
        repo['darren68_gladding_ralcalde.AdvancedClusterAnalysis'].insert_many(dicList)
        repo['darren68_gladding_ralcalde.AdvancedClusterAnalysis'].metadata({'complete': True})

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}




    @staticmethod
    def getMeaningfulYears(list_of_Changes, strdDev):
        meaningfulYears = []
        for i in range(len(list_of_Changes)):
            if abs(list_of_Changes[i]) > 1.5 * strdDev:
                meaningfulYears += [2003 + i]
        return meaningfulYears


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
        this_script = doc.agent('alg:darren68_gladding_ralcalde#AdvancedAnalyzeClusters',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # Resource = All Towns Data
        resource = doc.entity('dat:darren68_gladding_ralcalde#ClusterEvolution',
                              {'prov:label': 'Clusters', prov.model.PROV_TYPE: 'ont:DataSet', 'ont:Extension': 'json'})

        # Activity
        get_clus_change = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_clus_change, this_script)
        doc.usage(get_clus_change, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation',
                   'ont:Query': ''
                   }
                  )

        AdvancedClusterAnalysis = doc.entity('dat:darren68_gladding_ralcalde#AdvancedClusterAnalysis',
                                             {prov.model.PROV_LABEL: 'Analyze Clusters',
                                              prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(AdvancedClusterAnalysis, this_script)
        doc.wasGeneratedBy(AdvancedClusterAnalysis, get_clus_change, endTime)
        doc.wasDerivedFrom(AdvancedClusterAnalysis, resource, get_clus_change, get_clus_change, get_clus_change)

        repo.logout()

        return doc

