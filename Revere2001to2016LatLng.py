import urllib.request
import dml
import prov.model
import datetime
import uuid
import csv
import io





keynames = ['ageofdriveroldestknown', 'ageofdriveryoungestknown', 'ageofnonmotoristoldestknown',
            'ageofnonmotoristyoungestknown', 'ambientlight', 'citytown', 'county', 'crashhour', 'crashnumber',
            'crashseverity', 'crashstatus', 'datetime', 'distanceanddirectionfromexitnumb',
            'distanceanddirectionfromintersec', 'distanceanddirectionfromlandmark', 'distanceanddirectionfrommilemark',
            'drivercontributingcodes', 'driverdistractedby', 'exitnumber', 'exitroute',
            'federalfunctionalclassification', 'firstharmfulevent', 'firstharmfuleventlocation', 'fmscareportable',
            'geocodingmethod', 'hitrun', 'isgeocoded', 'landmark', 'latitude', 'linkedriaccesscontrol',
            'linkedriaveragedailytraffic', 'linkedrifacilitytype', 'linkedrifunctionalclassification',
            'linkedrijurisdiction', 'linkedrinumberoftravellanes', 'linkedriopposingnumberoftravella',
            'linkedrispeedlimit', 'linkedristreetname', 'linkedristreetoperation', 'linkedritollroad',
            'linkedritruckroute', 'linkedriurbanizedarea', 'linkedriurbanlocationtype', 'linkedriurbantype',
            'linkedriyearofadt', 'locality', 'longtitude', 'mannerofcollision', 'masshighwaydistrict',
            'maximuminjuryseverityreported', 'milemarker', 'milemarkerroute', 'mostharmfulevents',
            'nearintersectionroadway', 'nonmotoristaction', 'nonmotoristlocation', 'nonmotoristtype',
            'numberoffatalinjuries', 'numberofnonfatalinjuries', 'numberofvehicles', 'policeagency',
            'reportids', 'rmvdocument', 'roadsegmentid', 'roadsurface', 'roadway', 'roadwaycontributingcode',
            'roadwayintersectiontype', 'rpaabbreviation', 'schoolbusrelated', 'speedlimit', 'streetnumber',
            'trafficcontroldevicetype', 'trafficdevicefunctioning', 'trafficway', 'vehicleactionpriortocrash',
            'vehicleconfiguration', 'vehiclesequenceofevents', 'vehicletraveldirections', 'weathercondition']





class Revere2001to2016LatLng(dml.Algorithm):
    contributor = 'darren68_gladding_ralcalde'
    reads = []
    writes = ['darren68_gladding_ralcalde.Revere2001to2016LatLng']

    @staticmethod
    def execute(trial=False):
        '''Retrieve data set of crashes in Revere for the year 2001 to 2016 and store it in Mongo'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('darren68_gladding_ralcalde', 'darren68_gladding_ralcalde')

        # this is the url of the dataset
        url = 'http://datamechanics.io/data/darren68_gladding_ralcalde/Revere2001to2016LatLng.csv'

        # load the url and read it
        response = urllib.request.urlopen(url)
        file = csv.reader(io.StringIO(response.read().decode('utf-8')), delimiter=',')

        # skip the headers
        next(file, None)

        #dictList = []
        isNotFirst = False

        repo.dropCollection("Revere2001to2016LatLng")
        repo.createCollection("Revere2001to2016LatLng")

        # iterate through each row in the file and assign each element to the corresponding field in the dictionary
        i = 1
        for row in file:
            dic = {}
            dic[keynames[0]] = row[0]
            dic[keynames[1]] = row[1]
            dic[keynames[2]] = row[2]
            dic[keynames[3]] = row[3]
            dic[keynames[4]] = row[4]
            dic[keynames[5]] = row[5]
            dic[keynames[6]] = row[6]
            dic[keynames[7]] = row[7]
            dic[keynames[8]] = row[8]
            dic[keynames[9]] = row[9]
            dic[keynames[10]] = row[10]
            dic[keynames[11]] = datetime.datetime.strptime(row[11], '%m/%d/%Y %H:%M')
            dic[keynames[12]] = row[12]
            dic[keynames[13]] = row[13]
            dic[keynames[14]] = row[14]
            dic[keynames[15]] = row[15]
            dic[keynames[16]] = row[16]
            dic[keynames[17]] = row[17]
            dic[keynames[18]] = row[18]
            dic[keynames[19]] = row[19]
            dic[keynames[20]] = row[20]
            dic[keynames[21]] = row[21]
            dic[keynames[22]] = row[22]
            dic[keynames[23]] = row[23]
            dic[keynames[24]] = row[24]
            dic[keynames[25]] = row[25]
            dic[keynames[26]] = row[26]
            dic[keynames[27]] = row[27]
            dic[keynames[28]] = row[28]
            dic[keynames[29]] = row[29]
            dic[keynames[30]] = row[30]
            dic[keynames[31]] = row[31]
            dic[keynames[32]] = row[32]
            dic[keynames[33]] = row[33]
            dic[keynames[34]] = row[34]
            dic[keynames[35]] = row[35]
            dic[keynames[36]] = row[36]
            dic[keynames[37]] = row[37]
            dic[keynames[38]] = row[38]
            dic[keynames[39]] = row[39]
            dic[keynames[40]] = row[40]
            dic[keynames[41]] = row[41]
            dic[keynames[42]] = row[42]
            dic[keynames[43]] = row[43]
            dic[keynames[44]] = row[44]
            dic[keynames[45]] = row[45]
            dic[keynames[46]] = row[46]
            dic[keynames[47]] = row[47]
            dic[keynames[48]] = row[48]
            dic[keynames[49]] = row[49]
            dic[keynames[50]] = row[50]
            dic[keynames[51]] = row[51]
            dic[keynames[52]] = row[52]
            dic[keynames[53]] = row[53]
            dic[keynames[54]] = row[54]
            dic[keynames[55]] = row[55]
            dic[keynames[56]] = row[56]
            dic[keynames[57]] = row[57]
            dic[keynames[58]] = row[58]
            dic[keynames[59]] = row[59]
            dic[keynames[60]] = row[60]
            dic[keynames[61]] = row[61]
            dic[keynames[62]] = row[62]
            dic[keynames[63]] = row[63]
            dic[keynames[64]] = row[64]
            dic[keynames[65]] = row[65]
            dic[keynames[66]] = row[66]
            dic[keynames[67]] = row[67]
            dic[keynames[68]] = row[68]
            dic[keynames[69]] = row[69]
            dic[keynames[70]] = row[70]
            dic[keynames[71]] = row[71]
            dic[keynames[72]] = row[72]
            dic[keynames[73]] = row[73]
            dic[keynames[74]] = row[74]
            dic[keynames[75]] = row[75]
            dic[keynames[76]] = row[76]
            dic[keynames[77]] = row[77]
            dic[keynames[78]] = row[78]
            dic[keynames[79]] = row[79]

            repo['darren68_gladding_ralcalde.Revere2001to2016LatLng'].insert(dic)
            i += 1


        repo['darren68_gladding_ralcalde.Revere2001to2016LatLng'].metadata({'complete': True})

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
        this_script = doc.agent('alg:darren68_gladding_ralcalde#Revere2001to2016LatLng',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # Resource = All Towns Data
        resource = doc.entity('dat:darren68_gladding_ralcalde',
                              {'prov:label': 'Crashes', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        # Activity
        get_crashes = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crashes, this_script)

        doc.usage(get_crashes, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )

        crashes = doc.entity('dat:darren68_gladding_ralcalde#Revere2001to2016LatLng',
                             {prov.model.PROV_LABEL: 'Crashes', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crashes, this_script)
        doc.wasGeneratedBy(crashes, get_crashes, endTime)
        doc.wasDerivedFrom(crashes, resource, get_crashes, get_crashes, get_crashes)

        repo.logout()

        return doc
