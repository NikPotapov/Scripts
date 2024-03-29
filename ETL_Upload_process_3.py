from config.config import DWH, SHAREPOINT
from dateutil.relativedelta import relativedelta
import os
import pandas as pd
import datetime


def main():
    from datetime import date

    dwh = DWH()
    engine = dwh.engine()

    sharepoint = SHAREPOINT()
    s = sharepoint.connecting()

    today = date.today()
    month = today.month

    print(str(month))

    if(month < 10):
        this_month = str(today.year)  + '-0' + str(today.month)
    else:
        this_month = str(today.year)  + '-' + str(today.month)

    last_month_last_day = date(today.year, today.month, 1) - relativedelta(days=1)
    last_month = last_month_last_day.strftime('%Y-%m-%d')
    last_month_to_check = last_month_last_day.strftime('%Y-%m')
    last_month_last_second = last_month + ' 23:59:59'

    print(this_month)
    

    r = s.get(sharepoint.p31_1014_URL)
    baseurl = sharepoint.BASE_URL

    jsonreturn = r.json()
    files = []

    for file in jsonreturn['d']['results']:
        if(this_month in file['Name']):
            files.append(file)
            print(file['Name'])
            print('')
            requestedFile = s.get(baseurl+'/'+file['ServerRelativeUrl'])
            content = requestedFile.content

            with open('data.xlsx', 'wb') as output:
                output.write(content)

            print('Successfuly downloaded!')
            print('')

            xl = pd.ExcelFile('data.xlsx')
            df = xl.parse('Change Tasks', header = 2, skiprows = 0)
            df = df.loc[df['Title'] == '[P3-1014-Documentation]']
            df = df.loc[df['Closure Code'].isin([1, 2, 3])]

            

            df_new = pd.DataFrame()

            closure_comment = 'None'
            id = 0
            for index, row in df.iterrows():
                id = id + 1
                task = df.loc[index, 'Description'].split(':')
                status = df.loc[index, 'Closure Code']
                servers = task[4].split(',')
                servers = [s.replace('“', '') for s in servers]
                servers = [s.replace(' ', '') for s in servers]
                servers.remove('Datum')
                application = task[1].replace('“', '').split(',')[0]
                deployer = task[2].replace('“', '').split(',')[0]
                staging = task[3].replace('“', '').split(',')[0]
                date = df.loc[index, 'Planned Start (Timezone based)']
                version = task[6].replace('“', '').split(',')[0]
                comment = task[7].replace('“', '').replace(']', '').split(',')[0]
                if(df.loc[index,'Closure Code'] == 3): ## Failed
                    closure_comment = df.loc[index,'Closure Description']

                for server in servers:
                        list = [{'Server': server, 'Deployment ID': id ,'Application': application, 'Deployer': deployer, 'StagingInstanz:': staging, 'Date':date, 'Vesrion':version, 'Comment':comment, 'Closure Comment': closure_comment,'status': status, 'EndOfMonth':last_month_last_second,'Upload date':today}]
                        df_temp = pd.DataFrame(list)
                        df_new = df_new.append(df_temp)


            unique_dates = df_new['Date'].unique()
            df_new['Number of deployments'] = 0
            df_new = df_new.reset_index()

            for date in unique_dates:
                n_deploy = 0

                for index, row in df.iterrows():
                    if(df.loc[index, 'Planned Start (Timezone based)'] == date):
                        n_deploy = n_deploy + 1

                for index, row in df_new.iterrows():
                    if(df_new.loc[index, 'Date'] == date):
                        df_new.at[index, 'Number of deployments'] = n_deploy


            del(df_new['index'])

            try:
                print('Loading data into table [dbo].[MAN_p31_1014]...')
                df_new.to_sql('MAN_p31_1014', engine, if_exists='append',index=False)
                print('Done! Loaded rows, columns: {}'.format(df_new.shape))
                print('')
            except Exception as e:
                print(e)

            try:
                os.remove('data.xlsx')
            except Exception as e:
                print(e)

    # the below block makes sure that the script fails if there are no corresponding files in the directory
    if not files:
        print('')
        print('*************************************************************************')
        print('NO FILES FOUND MATCHING THE PATTERN *{}*.csv. EXITING SCRIPT...'.format(this_month))
        print('*************************************************************************')
        print('')
        raise ValueError



if __name__ == '__main__':
    main()
