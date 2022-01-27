from config.config import DWH, SHAREPOINT
import pandas as pd
import datetime
import os

## Prerequisities: MS Flow has to unzip the new files uploaded on Sharepoint (triggered automatically)

def main():

    dwh = DWH()
    engine = dwh.engine()
    sharepoint = SHAREPOINT()
    s = sharepoint.connecting()
    url = sharepoint.VCENTER_URL
    baseurl = sharepoint.BASE_URL

    r = s.get(url)
    jsonreturn = r.json()

    #search sharepoint vCenter folder for new monthly folders
    print('Looking for BB, EU10 and EU11 folders...')
    for folder in jsonreturn['d']['results']:
        if 'BB' in folder['Name']:
            print('*****Folder found: {}. Opening...*****'.format(folder['Name']))
            datacenter = 'BB'
            environment = 'BuildingBlock'
            url2 = url + '(\'' + folder['Name'] + '\')/Files'
            requestedFolder = s.get(url2)
            jsonreturn2 = requestedFolder.json()

            #list all the .csv files for report monitoring that are to be loaded into dwh
            for file in jsonreturn2['d']['results']:
                print('Processing file: {} ...'.format(file['Name']))
                table = file['Name'].split('_')[1].split('.')[0]
                # create a temporary file in automation server and load the data from sharepoint files to it.
                temp_file = '/srv/reporting/sharepoint/temp_vcenter_data.csv'

                requestedFile = s.get(sharepoint.BASE_URL +"/"+file['ServerRelativeUrl'])
                content = requestedFile.content

                #open the temorary file and populate it with the content from the respective file on sharepoint
                with open(temp_file, 'wb') as output:
                    output.write(content)

                df = pd.read_csv(temp_file, ';', encoding="utf-8")
                df['timestamp'] = datetime.datetime.now().date()
                df['datacenter'] = datacenter
                df['environment'] = environment
                if 'MemoryTotalMB' in df.columns:
                     df['MemoryTotalMB']= df['MemoryTotalMB'].replace(',','.', regex=True)
                if 'HostName' in df.columns:
                    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!' + str(temp_file))
                    df.dropna(subset=['HostName'], inplace=True)
                df.columns = df.columns.str.replace('(', '')
                df.columns = df.columns.str.replace(')', '')
                df.columns = df.columns.str.replace('"', '')
                df = df.drop_duplicates()

                print(df.head())
                print('')
                print('Dataframe columns are of the following datatypes:')
                print(df.dtypes)
                print('')

                print('Uploading to MAN_vSphere_{}...'.format(table))
                df.to_sql("MAN_vSphere_"+ table, engine, if_exists='append', index=False)
                print('Successfuly uploaded to DWH! Number of rows, columns: ' + str(df.shape))
                print('******************************************************')


        if 'EU10u11' in folder['Name']:
            print('*****Folder found: {}. Opening...*****'.format(folder['Name']))
            datacenter = 'EU10 & EU11 Consolidated'
            environment = 'PCEE'
            url2 = url + '(\'' + folder['Name'] + '\')/Files'
            requestedFolder = s.get(url2)
            jsonreturn2 = requestedFolder.json()

            #list all the .csv files for report monitoring that are to be loaded into dwh
            for file in jsonreturn2['d']['results']:
                print('Processing file: {} ...'.format(file['Name']))
                table = file['Name'].split('_')[1].split('.')[0]
                
                # create a temporary file in automation server and load the data from sharepoint files to it.
                temp_file = '/srv/reporting/sharepoint/temp_vcenter_data.csv'

                requestedFile = s.get(sharepoint.BASE_URL +"/"+file['ServerRelativeUrl'])
                content = requestedFile.content

                #open the temorary file and populate it with the content from the respective file on sharepoint
                with open(temp_file, 'wb') as output:
                    output.write(content)

                df = pd.read_csv(temp_file, ';', encoding="utf-8")
                df['timestamp'] = datetime.datetime.now().date()
                df['datacenter'] = datacenter
                df['environment'] = environment
                
                if 'HostName' in df.columns:
                    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!' + str(temp_file))
                    df.dropna(subset=['HostName'], inplace=True)
                df.columns = df.columns.str.replace('(', '')
                df.columns = df.columns.str.replace(')', '')
                df.columns = df.columns.str.replace('"', '')
                df = df.drop_duplicates()

                print(df.head())
                print('')
                print('Dataframe columns are of the following datatypes:')
                print(df.dtypes)
                print('')

                print('Uploading to MAN_vSphere_{}...'.format(table))
                df.to_sql("MAN_vSphere_"+ table, engine, if_exists='append', index=False)
                print('Successfuly uploaded to DWH! Number of rows, columns: ' + str(df.shape))
                print('******************************************************')


if __name__ == '__main__':
    main()
