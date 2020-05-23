import numpy as np 
import pandas as pd
import os 
import sys 
import io
import re 

from datetime import datetime 
from datetime import timedelta 
import pytz 
import time

from google.cloud import storage
import creds

if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

def startDateDefault(start, dateFormat):
        return datetime.strptime(start, dateFormat).date()
    
def getToday():
    return datetime.now(tz=pytz.timezone('America/New_York'))

def endDateDefault():
    return datetime.date(getToday())

class Covid19Data():
    '''
    Data Sources: 
        1. US County Level: NYTimes Github
            https://github.com/nytimes/covid-19-data
        2. US - Total + State Level: 
            https://covidtracking.com/api
        3. China - Total + Province Level: DXY-Covid-19-Data
            https://github.com/BlankerL/DXY-COVID-19-Data
        4. Other Country - Total + Province: JHU Github vs. DXY-Covid-19-Data
            https://github.com/CSSEGISandData/COVID-19
            https://github.com/BlankerL/DXY-COVID-19-Data
        5. Rumors: DXY-Covid-19-Dat
            https://github.com/BlankerL/DXY-COVID-19-Data
        6. News: Postman APIs, RSS Feeds > WSJ, MW, Bloomberg, NYTimes, Nikkei

    Storage Folder:
        - archive
        - staging 
        - target
    '''

    sources = {
            "NyTimes": {
                "County": "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"
            },
            "CovidTracking": {
                "StateHistorical": "https://covidtracking.com/api/v1/states/daily.csv",
                "CountryHistorical": "https://covidtracking.com/api/v1/us/daily.csv"
            },
            "DxyCovid19": {
                "Region": "https://raw.githubusercontent.com/BlankerL/DXY-COVID-19-Data/master/csv/DXYArea.csv",
                "Country": "https://raw.githubusercontent.com/BlankerL/DXY-COVID-19-Data/master/csv/DXYOverall.csv"
            },
            "JHU": {
                "ProvinceState": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/",
                "FilenameFormat": "%m-%d-%Y.csv",
                "StartDate": "01-22-2020"
            }
        }
        # removed sources:
        # ["CovidTracking"]["StateCurrent"] = "https://covidtracking.com/api/v1/states/current.csv"
        # ["CovidTracking"]["CountryCurrent"] = "https://covidtracking.com/api/v1/us/current.csv",
        # ["DXY-Covid-19"]["News"] = "https://raw.githubusercontent.com/BlankerL/DXY-COVID-19-Data/master/csv/DXYNews.csv"
    
    def __init__(self, startDate=endDateDefault() - timedelta(days=10), endDate=endDateDefault(), currentDate=None):
        # startDateDefault('01/22/2020', '%m/%d/%Y')
        self.startDate = startDate
        self.endDate = endDate
        self.currentDate = startDate

    def getFilename(self, key, url, regex=r'(?i).*[/]((.*)[.]csv)', group=1, desc=''):
        reg = re.match(r'(?i).*[/]((.*)[.]csv)', url)
        filename = key.lower() \
            + (('-' + desc) if desc else '') \
            + '-' + reg.group(1)
        return filename
    
    def getStagingBlob_andArchiveCurrentTarget(self, bucket, folderStaging, folderTarget, folderArchive, filename, getStaging=True, getTarget=True):
        # create blobs
        blobStaging = bucket.blob(folderStaging + filename)
        blobTarget = bucket.blob(folderTarget + filename)

        archiveFilename = re.match(r'(?i)(.*)[.]csv', filename).group(1)
        date = datetime.today().strftime("%Y%m%d")
        archiveFilename = '%s-%s.csv' % (archiveFilename, date)
        blobArchive = bucket.blob(folderArchive + archiveFilename)

        # retrive file from staging and convert to Dataframe
        df = pd.DataFrame([])
        if getStaging:
            downloadStaging = blobStaging.download_as_string()
            print("Retrieved file from staging: %s" % filename)

            downloadStaging = StringIO(str(downloadStaging, "utf-8"))
            df = pd.read_csv(downloadStaging, delimiter=",")

        if getTarget:
            # retrieve/archive current file in target
            try:
                downloadTarget = blobTarget.download_as_string()
                blobArchive.upload_from_string(downloadTarget, content_type="text/csv")
                print("Target file archived: %s > %s" % (filename, archiveFilename))

                del [blobArchive, archiveFilename, date, downloadTarget]
            except Exception as e:
                print("Archival Skipped. Target file does not exist: %s\n" % filename)
                print("ERROR: %s" % e)

        return df, blobTarget

    def getData(self, source=None, level=None):
        client, bucket_name, staging, target, archive, log = creds.storageClient()
        bucket = client.bucket(bucket_name)
        bucket.versioning_enabled = False

        for k in self.sources:
            if k == "JHU":
                self.getJhuData(bucket, staging, self.sources[k]["ProvinceState"], self.startDate, self.endDate)
            else: 
                for j in self.sources[k]:
                    try: 
                        # fetch CSV
                        url = self.sources[k][j]
                        df = pd.read_csv(url, error_bad_lines=False, delimiter=',')
                        print("Fetched file for %s - %s" % (k, j))

                        # push to GCS
                        df = df.to_csv(index=False)
                        filename = self.getFilename(k, url, desc=j)

                        blob = bucket.blob(staging + filename)
                        blob.upload_from_string(df, content_type="text/csv")
                        print("Pushed file for %s - %s to GCS" % (k, j))
                    except Exception as e:
                        print("ERROR: failed to fetch/push %s - %s" % (k, j))
                        print(e)
    
    def getJhuData(self, bucket, folder, baseURL, startDate=None, endDate=None, currentDate=None):
        if startDate == None:
            startDate = self.startDate
        if endDate == None:
            endDate = self.endDate
        if currentDate == None:
            currentDate = self.currentDate
        while currentDate <= endDate:
            currentDateStr = currentDate.strftime('%m-%d-%Y')
            url = baseURL + currentDateStr + '.csv'
            try:
                # fetch CSV
                df = pd.read_csv(url, error_bad_lines=False, delimiter=',')
                print("Fetched file for JHU - %s" % currentDateStr)

                # push to GCS
                df = df.to_csv(index=False)
                filename = "jhu-" + currentDateStr + '.csv'

                blob = bucket.blob(folder + filename)
                blob.upload_from_string(df, content_type="text/csv")
                print("Pushed file for JHU - %s to GCS" % currentDateStr)
            except Exception as e:
                print(e)
            finally:
                currentDate = currentDate + timedelta(days=1)

    def processData(self):
        client, bucket_name, staging, target, archive, log = creds.storageClient()
        bucket = client.bucket(bucket_name)
        bucket.versioning_enabled = False

        for k in self.sources:
            self.process(source=k, bucket=bucket, staging=staging, target=target, archive=archive)
    
    def process(self, **kwargs):
        """Retrieves current file from staging, archives, existing target files, processes staging file and pushes it to the target folder. 

        ## Required Keyword Arguments:
        - source -- key in the sources objects.
        - bucket -- GCS bucket object
        - staging -- path to staging folder
        - target -- path to target folder
        - archive -- path to archive folder
        """
        print("Processing %s" % kwargs['source'])

        if kwargs['source'] == 'JHU':
            # get target filename
            filename = 'jhu-target.csv'

            # get 1) files, 2) combine files into one Dataframe object 3) get blobTarget 4) archive current file in target
            source = kwargs['source']
            startDate = datetime.strptime(self.sources[source]['StartDate'], '%m-%d-%Y').date()
            endDate = datetime.date(datetime.now(tz=pytz.timezone('Etc/GMT')))
            
            df, blobTarget = self.getStagingBlob_andArchiveCurrentTarget(kwargs['bucket'], kwargs['staging'], kwargs['target'], kwargs['archive'], filename, getStaging=False)

            currentDate = startDate
            while currentDate <= endDate:
                currentDateStr = currentDate.strftime('%m-%d-%Y')
                print("attempting current date = %s" % currentDateStr)
                blobStaging = kwargs['bucket'].blob(kwargs['staging'] + \
                    '%s-%s.csv' % (source.lower(), currentDateStr))
                print('created blob: %s' % currentDateStr)
                
                downloadedFile = ''
                try:
                    downloadedFile = str(blobStaging.download_as_string(), encoding="utf-8")
                    print('downloaded file: %s' % currentDateStr)
                except Exception as e:
                    print("ERROR: file not found - %s" % currentDateStr)
                    print(e)

                if len(downloadedFile) == 0:
                    currentDate = currentDate + timedelta(days=1)
                    print("Empty file: %s" % currentDateStr)
                    continue

                csvFile = StringIO(downloadedFile)
                tempDf = pd.read_csv(csvFile, delimiter=",")
                print("Converted download to DF - %s" % currentDateStr)
                
                df = df.append(tempDf)
                print("Appended file to DF: %s" % currentDateStr)
                currentDate = currentDate + timedelta(days=1)

            # process combined df object
            f = eval('self.process%s%s' % (source, 'ProvinceState'))
            df = f(df)
            print("Finish processing data: %s" % filename)

            # push processed df to target
            df = df.to_csv(index=False)
            blobTarget.upload_from_string(df, content_type="text/csv")
            print("Pushed file to Target: %s" % filename)

            return

        for key in self.sources[kwargs['source']]:
            # get filenames
            filename = self.getFilename(kwargs['source'], self.sources[kwargs['source']][key], desc=key)

            # retrieve files from staging, archive existing target files
            df, blobTarget = self.getStagingBlob_andArchiveCurrentTarget(kwargs['bucket'], kwargs['staging'], kwargs['target'], kwargs['archive'], filename)

            # process staging file
            f = eval('self.process%s%s' % (kwargs['source'], key))
            df = f(df)
            print("Finish processing data: %s" % filename)

            # push to target
            df = df.to_csv(index=False)
            blobTarget.upload_from_string(df, content_type="text/csv")
            print("Pushed file to Target: %s" % filename)
    
    def processNyTimesCounty(self, df):
        return df

    def processJHUProvinceState(self, df):
        # rename columns
        df.rename(columns = {
            "Admin2": 'US_County'
        }, inplace=True)

        # combine duplicate columns
        df['LastUpdate'] = df.apply(lambda row: row['Last Update'] if type(row['Last Update']) == str else row['Last_Update'], axis=1)
        df['CountryRegion'] = df.apply(lambda row: row['Country/Region'] if type(row['Country/Region']) == str else row['Country_Region'], axis=1)
        df['StateProvince'] = df.apply(lambda row: row['Province/State'] if type(row['Province/State']) == str else row['Province_State'], axis=1)

        # fix Lat/Long columns and combine
        df.Lat = df.Lat.fillna(0)
        df.Latitude = df.Latitude.fillna(0)
        df.Long_ = df.Long_.fillna(0)
        df.Longitude = df.Longitude.fillna(0)
        df['Latitude'] = df.apply(lambda row: row['Lat'] if row['Lat'] != 0 else row['Latitude'], axis=1)
        df['Longitude'] = df.apply(lambda row: row['Long_'] if row['Long_'] != 0 else row['Longitude'], axis=1)

        # convert String date columns and combine
        def convertLastUpdate(df): 
            reg_mmddyyHHMM = r'^[0-9]{1,2}[/][0-9]{1,2}[/][0-9]{2} [0-9]{1,2}:[0-9]{1,2}$'
            reg_mmddyyyyHHMM = r'^[0-9]{1,2}[/][0-9]{1,2}[/][0-9]{4} [0-9]{1,2}:[0-9]{1,2}$'
            reg_yyyymmddTHHMMSS = r'^[0-9]{4}[-][0-9]{1,2}[-][0-9]{1,2}T[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}$'
            reg_yyyy_mm_dd_HHMMSS = r'^[0-9]{4}[-][0-9]{1,2}[-][0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}$'

            mmddyyyyHHMM = '%m/%d/%Y %H:%M'
            mmddyyHHMM = '%m/%d/%y %H:%M'
            yyyymmddTHHMMSS = '%Y-%m-%dT%H:%M:%S'
            yyyy_mm_dd_HHMMSS = '%Y-%m-%d %H:%M:%S'

            if re.match(reg_mmddyyHHMM, df.LastUpdate):
                return datetime.strptime(df.LastUpdate, mmddyyHHMM)
            elif re.match(reg_mmddyyyyHHMM , df.LastUpdate):
                return datetime.strptime(df.LastUpdate, mmddyyyyHHMM)
            elif re.match(reg_yyyymmddTHHMMSS , df.LastUpdate):
                return datetime.strptime(df.LastUpdate, yyyymmddTHHMMSS)
            elif re.match(reg_yyyy_mm_dd_HHMMSS, df.LastUpdate):
                return datetime.strptime(df.LastUpdate, yyyy_mm_dd_HHMMSS)
            else:
                return None
        df['LastUpdateDatetime'] = df.apply(convertLastUpdate, axis=1)
        df['LastUpdateDate'] = df.LastUpdateDatetime.dt.date

        # delete unnecessary columns
        colsToDelete = ['Last_Update', 'Last Update', 'FIPS', 'Country/Region', 'Country_Region', 'Unnamed: 0', 'Long_', 'Lat', 'Province/State', 'Province_State', 'LastUpdate', 'Combined_Key', 'updateLength']
        for col in colsToDelete:
            try:
                df = df.drop([col], axis=1)
            except:
                pass
        
        # set categorical columns
        colsCategory = ['Latitude', 'Longitude', 'US_County', 'StateProvince', 'CountryRegion', 'LastUpdateDatetime', 'LastUpdateDate']
        for col in colsCategory:
            df[col] = df[col].astype('category')

        # remove China from dataset (using another data source for China)
        df = df[df.CountryRegion.isin(['China', 'Mainland China']) == False]

        return df

    def processCovidTrackingStateHistorical(self, df):
        colsToDrop = ['hash', 'posNeg', 'fips', 'dateChecked']
        df = df.drop(colsToDrop, axis=1)
        df['date'] = pd.to_datetime(df.date, format='%Y%m%d').dt.date
        for col in ['date', 'state']:
            df[col] = df[col].astype('category')
        return df
    
    def processCovidTrackingCountryHistorical(self, df):
        colsToDrop = ['hash', 'posNeg', 'fips', 'dateChecked', 'states']
        df = df.drop(colsToDrop, axis=1)
        df['date'] = pd.to_datetime(df.date, format='%Y%m%d').dt.date
        for col in ['date']:
            df[col] = df[col].astype('category')
        return df

    def processDxyCovid19Country(self, df):
        colsToKeep = ['confirmedCount', 'suspectedCount', 'curedCount', 'deadCount', 'seriousCount', 'updateTime']
        df = df[colsToKeep]
        df.rename(columns= {
            'confirmedCount': 'confirmed', 
            'suspectedCount': 'suspected',
            'curedCount': 'recovered', 
            'deadCount': 'death', 
            'seriousCount': 'serious'
        }, inplace=True)
        df['date'] = pd.to_datetime(df.updateTime, format='%Y-%m-%d %H:%M:%S').dt.date

        for col in ['updateTime', 'date']:
            df[col] = df[col].astype('category').cat.as_ordered()

        def chooseLatestRow(df):
            return df[df.updateTime == df.updateTime.max()]
        
        df = df.groupby(['date']).apply(chooseLatestRow)
        df = df[['confirmed', 'suspected', 'recovered', 'death', 'serious']]\
            .reset_index()\
            [['date', 'confirmed', 'suspected', 'recovered', 'death', 'serious']]

        return df

    def processDxyCovid19Region(self, df):
        df = df.drop(['continentName', 'countryName', 'provinceName', 'cityName'], axis=1)
        df.rename(columns = {
            'continentEnglishName': 'continent', 
            'countryEnglishName': 'country', 
            'provinceEnglishName': 'province', 
            'cityEnglishName': 'city'
        }, inplace=True)
        df['date'] = pd.to_datetime(df.updateTime, format='%Y-%m-%d %H:%M:%S').dt.date
        validCountries = ['China']
        df = df[(df.country.isin(validCountries))].groupby(['continent', 'country', 'province', 'province_zipCode', 'date', 'updateTime']).agg({
            'province_confirmedCount': 'max',
            'province_suspectedCount': 'max',
            'province_curedCount': 'max',
            'province_deadCount': 'max'
        }).reset_index().groupby(['continent', 'country', 'province', 'date', 'updateTime']).agg({
            'province_confirmedCount': 'sum',
            'province_suspectedCount': 'sum',
            'province_curedCount': 'sum',
            'province_deadCount': 'sum'
        }).reset_index().copy()

        for col in ['continent', 'country', 'province', 'date', 'updateTime']:
            df[col] = df[col].astype('category')

        df = df[df.province != 'China'].groupby(['continent', 'country', 'province', 'date']).agg({
            'province_confirmedCount': 'max',
            'province_suspectedCount': 'max',
            'province_curedCount': 'max',
            'province_deadCount': 'max'
        }).ffill().groupby(['continent', 'country', 'province', 'date']).cummax().reset_index()

        return df
