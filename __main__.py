import requests
import os,sys
from loguru import logger
import datetime
import getpass
from pathlib import Path
import json
import datetime
from humanize import precisedelta
from rich import print
import pycountry
from prettytable import PrettyTable
import re
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import exc
import urllib

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from doorKey import config

###pandas configs
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.colheader_justify', 'center')
pd.set_option('display.precision', 3)


class TooManyServiceTags(Exception):
    pass


class ServiceTagNotValid(Exception):
    pass


class SecretsInvalid(Exception):
    pass

class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, r: requests.models.PreparedRequest) -> requests.models.PreparedRequest:
        try:
            r.headers["authorization"] = "Bearer " + self.token
        except TypeError:
            os.remove(f'{str(Path.home())}/.cache.json')
            logger.info('cache file (with bad access token) has just been deleted')
        finally:
            # retry again
            try:
                r.headers["authorization"] = "Bearer " + self.token
            except Exception as e:
                logger('Something goes wrong! Original error {}', e)
                raise SecretsInvalid
        return r
    
class DellApi:
    def __init__(self):
        self._home = str(os.getcwd())
        
    def _generate_access_token(self):
        client_auth = requests.auth.HTTPBasicAuth(config['dell']['key'],config['dell']['secret'])
        response = requests.post('https://apigtwb2c.us.dell.com/auth/oauth/v2/token',
                                    auth=client_auth,
                                    data={"grant_type": "client_credentials"})

        access_token = response.json().get("access_token")
        print(access_token)
        timestamp = datetime.datetime.now()
        data = {'access_token': access_token,
        'timestamp': timestamp.isoformat()}

        with open(f'{self._home}/.cache.json', 'w') as j:
            json.dump(data, j)
            logger.debug('The access token has just received and saved to cache')
    
    def _load_access_token(self) -> dict:
        with open(f"{self._home}/.cache.json") as j:
            logger.debug('Loading access token from cache')
            return json.load(j)   
    
    def _is_token_valid(self, iso_date_string: str) -> bool:
        when_generated = datetime.datetime.fromisoformat(iso_date_string)
        now = datetime.datetime.now()
        diff_seconds = (now - when_generated).seconds
        logger.debug('Token valid for one hour, created at -> {}', when_generated)
        if diff_seconds >= 3600:
            return False
        else:
            return True   
    
    def _get_access_token(self) -> str:
        if not os.path.isfile(f"{self._home}/.cache.json"):
            logger.debug('Did not find cache file')
            self._generate_access_token()

        data = self._load_access_token()
        valid = self._is_token_valid(data['timestamp'])

        if valid:
            logger.debug('Access token is valid')
            return data['access_token']

        elif not valid:
            logger.debug('Access token is invalid, receiving new')
            self._generate_access_token()
            return self._load_access_token()['access_token']
        
    def asset_warranty(self, service_tags: list) -> list[dict]:
        if len(service_tags) > 99:
            raise TooManyServiceTags(f"Expected less then 100, got {len(service_tags)}")
        else:
            st = ','.join(service_tags)
        auth = BearerAuth(self._get_access_token())
        api_endpoint = f'https://apigtwb2c.us.dell.com/PROD/sbil/eapi/v5/asset-entitlements?servicetags={st}'
        response = requests.get(api_endpoint, auth=auth)
        answer = response.json()
        return answer
    
    def print_asset_warranty(self, service_tags: list):
        print(self.asset_warranty(service_tags))
    
    def asset_details(self, service_tag: str) -> dict:
        if isinstance(service_tag, list):
            logger.debug("Wrong type -> {}, {}", service_tag, type(service_tag))
            service_tag = service_tag[0]
        auth = BearerAuth(self._get_access_token())
        api_endpoint = f'https://apigtwb2c.us.dell.com/PROD/sbil/eapi/v5/asset-components?servicetag={service_tag}'
        response = requests.get(api_endpoint, auth=auth)
        answer = response.json()
        return answer

    def print_asset_details(self, service_tag: list):
        print(self.asset_details(service_tag))
    
    def _warranty_remains(self, expire_date: datetime.datetime) -> str:
        delta: datetime.timedelta = expire_date - datetime.datetime.utcnow()
        logger.debug('Diff between given date <{}-datetime> and today: <{} - timedelta>', expire_date, delta)

        if delta.days >= 0:
            remain = precisedelta(delta, minimum_unit='days', format='%0.0f')
            return remain

        else:
            return 'Expired'    
    
    def _strdate_datetime(self, date: str) -> datetime.datetime:
        regex = r'^[\d]{4}-[\d]{2}-[\d]{2}T[\d]{2}:[\d]{2}:[\d]{2}'
        match = re.match(regex, date, re.DOTALL)
        if match:
            return datetime.datetime.strptime(match.group(), '%Y-%m-%dT%H:%M:%S')
        else:
            logger.error("Arg {} does not match str <-> datetime format", date)
               
    def _service_tag_validate(self, service_tag: str) -> bool:
        if isinstance(service_tag, str) and re.match(r'^[\d|A-Z]{7}$', service_tag, re.DOTALL):
            return False
        else:
            return True   
        
    def servicetags_from_file(self, abspath) -> list:
        with open(f"{abspath}") as f:
            service_tags = [st.strip() for st in f]
            return service_tags
        
    def _service_tags_validate(self, service_tags: list) -> bool:
        for tag in service_tags:
            if not self._service_tag_validate(tag):
                return False
        else:
            return True
        
    def _warranty_type_handler(self, services: list) -> str:
        b, p, pp = ('Basic', 'ProSupport', 'ProSupport Plus')
        logger.debug('Warranty services -> {}', services)
        result = []

        for service in services:
            circle = [False, False]
            rp = lambda x: re.match(r'^.+?ProSupport Plus', x, re.DOTALL)
            rs = lambda x: re.match(r'^ProSupport', x, re.DOTALL)
            if rp(service):
                circle[0] = True
            if rs(service):
                circle[1] = True
            result.append(circle)

        pro_plus = [service[0] for service in result]
        pro = [service[1] for service in result]

        if True in pro_plus:
            return pp
        elif (True in pro) and (True not in pro_plus):
            return p
        elif (True not in pro_plus) and (True not in pro):
            return b
          
    def _warranty_handler(self, resp: list) -> list[dict]:
        logger.debug('data into handler: {}', resp)
        data = []  # ServiceTag, Region, Warranty, Elapsed, EndDate

        for tag in resp:

            try:
                st = tag['serviceTag']

                try:
                    region = pycountry.countries.get(alpha_2=tag['countryCode']).name
                except AttributeError:
                    if tag['countryCode'] == 'XM':
                        region = 'Hong Kong'
                    else:
                        region = tag['countryCode']
                        logger.warning('Could not parse country code -> {}', region)

                model = tag['productLineDescription']
                services = []
                services_start_dates = []
                services_end_dates = []
                # Searching for WarrantyType and End Date:

                for entitlement in tag['entitlements']:

                    if isinstance(entitlement['serviceLevelDescription'], type(None)):
                        continue

                    services_start_dates.append(entitlement['startDate'])
                    services_end_dates.append(entitlement['endDate'])
                    services.append(entitlement['serviceLevelDescription'])

                logger.warning("start dates: {}", services_start_dates)
                highest_date = lambda dates: sorted(list(map(self._strdate_datetime, dates))).pop()
                lowest_date = lambda dates: sorted(list(map(self._strdate_datetime, dates))).pop(0)
                warranty_start_date = lowest_date(services_start_dates)
                warranty_end_date = highest_date(services_end_dates)
                # warranty_end_date: datetime.datetime = sorted(
                # list(map(self._strdate_datetime, services_end_dates))).pop()

                remains = self._warranty_remains(warranty_end_date)
                warranty = self._warranty_type_handler(services)

                data.append({"ServiceTag": st,
                             "Model": model,
                            #  "Country": region,
                             "Warranty": warranty,
                            #  "Remain": remains,
                             "StartDate": warranty_start_date.strftime('%Y-%m-%d'),
                             "EndDate": warranty_end_date.strftime('%Y-%m-%d'),
                             })
            except Exception as e:
                logger.warning("Some error -> {}", e)
                data.append({"ServiceTag": tag['serviceTag'],
                            #  "Country": e,
                             "Warranty": '',
                            #  "Remain": '',
                             'StartDate': '',
                             "EndDate": '',
                             })
        logger.debug(data)
        df = pd.DataFrame(data)
        return data[::-1]
    
    def warranty_Dataframe(self, service_tags: list):
            if not self._service_tags_validate(service_tags):
                logger.error("Service Tags {}", service_tags)
                pass

            jsons = self.asset_warranty(service_tags)
            data = self._warranty_handler(jsons)
            df = pd.DataFrame(data)
            return df
            
    def warranty_table(self, service_tags: list):
        if not self._service_tags_validate(service_tags):
            logger.error("Service Tags {}", service_tags)
            raise ServiceTagNotValid

        jsons = self.asset_warranty(service_tags)
        data = self._warranty_handler(jsons)
        table = PrettyTable()
        table.field_names = [key for key in data[0]]
        for row in data:
            table.add_row([row[key] for key in row])
        print(table, f'Total: {len(data)}', sep='\n')

    def details_table(self, service_tag: str):
        if isinstance(service_tag, list):
            logger.debug("Wrong type -> {}, {}", service_tag, type(service_tag))
            service_tag = service_tag[0]

        if not self._service_tag_validate(service_tag):
            print('Found bad service tag.',service_tag)
            pass
        json = self.asset_details(service_tag)
        components = json["components"]
        table = PrettyTable()
        table.field_names = [key for key in components[0]]
        for component in components:
            table.add_row([component[key] for key in component])

        print(table)

    def warranty_json(self, service_tags: list):
        if not self._service_tags_validate(service_tags):
            raise ServiceTagNotValid

        jsons = self.asset_warranty(service_tags)
        data = self._warranty_handler(jsons)
        print(data)
        # return data


params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                'Server='+(config['database']['Server'])+';'
                                'Database=GCAAssetMGMT_2_0;'
                                'UID='+(config['database']['UID'])+';'
                                'PWD='+(config['database']['PWD'])+';')

conn = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

def df_to_sql(dataframe):
    with conn.connect() as connection:
        dataframe.to_sql(
            con=connection,
            schema="Asset",
            name="DellWarrantyInfo",
            if_exists="append",
            index=False
        )


statement = """
SELECT
    STUFF((SELECT','+SN
            FROM(
                SELECT TOP 99 LOWER(SerialNumber) AS SN 
                FROM GCAAssetMGMT_2_0.Asset.Info 
                WHERE Manufacturer = 'Dell'
                    AND LEN(SerialNumber) BETWEEN 6 and 8
                ORDER BY SerialNumber) s
            FOR XML PATH('')), 1,1,'')

"""
### Comment out if you want to hide
logger.remove(0)

def main():
    for i in range(100):
        sql_warranty_pull = f"EXEC GCAAssetMGMT_2_0.Asset.uspNxt100_4_DellWarrantyCheck;"
        warranty_sql_pull = pd.read_sql_query(sql_warranty_pull,conn)
        print(warranty_sql_pull)
        if not warranty_sql_pull.empty:
            print('Done')
            exit()
        json_file = warranty_sql_pull['result'].loc[0]
        serial_numbers = list(json_file.split(","))

        d = DellApi()
        wt = d.warranty_Dataframe(serial_numbers)
        
        if len(wt)>0:
            df_to_sql(wt)
            print("Loop",i,"Uploaded",len(wt),"warranties.")
          
        else:
            print('Done')
            exit()
            
def secondary():
    sql_warranty_pull = f"EXEC GCAAssetMGMT_2_0.Asset.uspNxt100_4_DellWarrantyCheck;"
    warranty_sql_pull = pd.read_sql_query(sql_warranty_pull,conn)
    json_info = warranty_sql_pull['result'].loc[0]
    # print(json_info)
    serial_numbers = list(json_info.split(","))
    # print(serial_numbers)
    d = DellApi()
    wt = d.warranty_Dataframe(serial_numbers)
    print(wt)

def singleServe():
    d = DellApi()
    service_tag=input('Enter service tag:')
    dt = d.details_table(service_tag)
        
main()
        
# js = warranty_sql_pull.to_json(orient = 'index')
# print(js)
# df = pd.DataFrame(js)
# print(df)
# serial_numbers = ['100B1Z2', '101Y433', '102H0B3', '102NKT2', '10300Z2']

# selection = input('1 for details of a service tag.\n2 for serial numbers\n')
# if selection == '1':
#     service_tag=input('Enter service tag:')
#     dt = d.details_table(service_tag)
# elif selection == '2':
#     service_tags = d.servicetags_from_file(r'C:\Users\Mbrown\Desktop\GCA-Coding\Projects\Python\sca_dell_warranty\serial.txt')
#     wt = d.warranty_Dataframe(serial_numbers)