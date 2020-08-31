import dml
import prov.model
import datetime
import uuid
import random



class StatsOnSeverity(dml.Algorithm):

    contributor = 'darren68_gladding_ralcalde'
    reads = ["darren68_gladding_ralcalde.Revere2001to2016"]
    writes= ['darren68_gladding_ralcalde.SeverityPercentageOfAccidents',
             'darren68_gladding_ralcalde.SeverityStats']
    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('darren68_gladding_ralcalde', 'darren68_gladding_ralcalde')

        colName = "darren68_gladding_ralcalde." + "Revere2001to2016"
        collection = repo[colName]

        docs = collection.find()

        # Pure Numbers
        Non_fatal = 0
        Property = 0
        FatalNumb = 0
        TotalQuant = 0

        # Lists of tuples with form (Year, )
        Nfatal = []
        Fatal = []
        Ninjury = []

        FullList = []

        for doc in docs:

            if doc['crashseverity'] == 'Non-fatal injury':
                date =  str(doc['datetime'].year)
                Nfatal += [(date, doc)]
                Non_fatal += 1
                TotalQuant += 1


            elif doc['crashseverity'] == 'Property damage only (none injured)':
                date = str(doc['datetime'].year)
                Ninjury += [(date, doc)]
                TotalQuant += 1
                Property += 1

            elif doc['crashseverity'] == 'Fatal injury':
                date = str(doc['datetime'].year)
                Fatal += [(date, doc)]
                TotalQuant += 1
                FatalNumb += 1


        dic = {"Non Fatal Percentage ": Non_fatal / TotalQuant * 100, "Fatal Percentage": FatalNumb / TotalQuant * 100,
               "Only Property Percentage": Property / TotalQuant * 100}

        repo.dropCollection("SeverityPercentageOfAccidents")
        repo.createCollection("SeverityPercentageOfAccidents")
        repo['darren68_gladding_ralcalde.SeverityPercentageOfAccidents'].insert(dic)
        repo['darren68_gladding_ralcalde.SeverityPercentageOfAccidents'].metadata({'complete': True})

        FatalaccidentsPerYear = StatsOnSeverity.aggregateYears(Fatal, sum)
        Non_fatalaccidentsPerYear = StatsOnSeverity.aggregateYears(Nfatal, sum)
        Non_InjuryaccidentsPerYear = StatsOnSeverity.aggregateYears(Ninjury, sum)

        FatalaccidentsPerYear = [(v, 'Fatal', k) for (v, k) in FatalaccidentsPerYear]
        Non_fatalaccidentsPerYear = [(v, 'Non-Fatal', k) for (v, k) in Non_fatalaccidentsPerYear]
        Non_InjuryaccidentsPerYear = [(v, 'No Injury', k) for (v, k) in Non_InjuryaccidentsPerYear]

        Fatal_changePerYear = []
        Fatal_adjustedChangePerYear = []
        for i in range(len(FatalaccidentsPerYear) - 1):
            Fatal_changePerYear += [FatalaccidentsPerYear[i + 1][2] - FatalaccidentsPerYear[i][2]]

        dictList = []

        for i in range(len(FatalaccidentsPerYear)):
            dic = {}
            dic['Year'] = FatalaccidentsPerYear[i][0]
            dic['Severity'] = FatalaccidentsPerYear[i][1]
            dic['Accidents'] = FatalaccidentsPerYear[i][2]

            if i != 0:
                dic['ChangeFromPrevious'] = Fatal_changePerYear[i - 1]
                dic['PercentageChange'] = float('Nan')
            else:
                dic['ChangeFromPrevious'] = float('Nan')
                dic['PercentageChange'] = float('Nan')

            dictList.append(dic)

        NonFatal_changePerYear = []
        NonFatal_adjustedChangePerYear = []

        for i in range(len(Non_fatalaccidentsPerYear) - 1):
            NonFatal_changePerYear += [Non_fatalaccidentsPerYear[i + 1][2] - Non_fatalaccidentsPerYear[i][2]]
            NonFatal_adjustedChangePerYear += [
                ((Non_fatalaccidentsPerYear[i + 1][2] / Non_fatalaccidentsPerYear[i][2]) - 1) * 100]

        for i in range(len(Non_fatalaccidentsPerYear)):
            dic = {}
            dic['Year'] = Non_fatalaccidentsPerYear[i][0]
            dic['Severity'] = Non_fatalaccidentsPerYear[i][1]
            dic['Accidents'] = Non_fatalaccidentsPerYear[i][2]

            if i != 0:
                dic['ChangeFromPrevious'] = NonFatal_changePerYear[i - 1]
                dic['PercentageChange'] = NonFatal_adjustedChangePerYear[i - 1]
            else:
                dic['ChangeFromPrevious'] = float('Nan')
                dic['PercentageChange'] = float('Nan')

            dictList.append(dic)

        NonInj_changePerYear = []
        NonInj_adjustedChangePerYear = []

        for i in range(len(Non_InjuryaccidentsPerYear) - 1):
            NonInj_changePerYear += [Non_InjuryaccidentsPerYear[i + 1][2] - Non_InjuryaccidentsPerYear[i][2]]
            NonInj_adjustedChangePerYear += [
                ((Non_InjuryaccidentsPerYear[i + 1][2] / Non_InjuryaccidentsPerYear[i][2]) - 1) * 100]

        for i in range(len(Non_InjuryaccidentsPerYear)):
            dic = {}
            dic['Year'] = Non_InjuryaccidentsPerYear[i][0]
            dic['Severity'] = Non_InjuryaccidentsPerYear[i][1]
            dic['Accidents'] = Non_InjuryaccidentsPerYear[i][2]

            if i != 0:
                dic['ChangeFromPrevious'] = NonInj_changePerYear[i - 1]
                dic['PercentageChange'] = NonInj_adjustedChangePerYear[i - 1]
            else:
                dic['ChangeFromPrevious'] = float('Nan')
                dic['PercentageChange'] = float('Nan')

            dictList.append(dic)

        repo.dropCollection("SeverityStats")
        repo.createCollection("SeverityStats")
        repo['darren68_gladding_ralcalde.SeverityStats'].insert_many(dictList)
        repo['darren68_gladding_ralcalde.SeverityStats'].metadata({'complete': True})

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
        this_script = doc.agent('alg:darren68_gladding_ralcalde#StatsOnSeverity',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # Resource = All Towns Data
        resource = doc.entity('dat:darren68_gladding_ralcalde#Revere2001to2016',
                              {'prov:label': 'Crashes', prov.model.PROV_TYPE: 'ont:DataSet', 'ont:Extension': 'json'})

        # Activity
        get_stats = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_severity_per = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_stats, this_script)
        doc.usage(get_stats, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation',
                   'ont:Query': ''
                   }
                  )
        doc.wasAssociatedWith(get_severity_per, this_script)

        doc.usage(get_severity_per, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation',
                   'ont:Query': ''
                   }
                  )

        SeverityPercentageOfAccidents = doc.entity('dat:darren68_gladding_ralcalde#SeverityPercentageOfAccidents', {
            prov.model.PROV_LABEL: 'Breakdown of Severity Percentage of Accidents',
            prov.model.PROV_TYPE: 'ont:DataSet'})
        SeverityStats = doc.entity('dat:darren68_gladding_ralcalde#SeverityStats',
                                   {prov.model.PROV_LABEL: 'Stats on Severity', prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(SeverityPercentageOfAccidents, this_script)
        doc.wasGeneratedBy(SeverityPercentageOfAccidents, get_severity_per, endTime)
        doc.wasDerivedFrom(SeverityPercentageOfAccidents, resource, get_severity_per, get_severity_per,
                           get_severity_per)

        doc.wasAttributedTo(SeverityStats, this_script)
        doc.wasGeneratedBy(SeverityStats, get_stats, endTime)
        doc.wasDerivedFrom(SeverityStats, resource, get_stats, get_stats, get_stats)

        repo.logout()

        return doc
