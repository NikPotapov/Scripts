from config.config import DWH, SHAREPOINT, TRANSFORMATION
import pandas as pd
import os
from datetime import datetime

''' uploads the daily IM, PM, Chgm and RF data from SC3 into the dwh '''


sharepoint = SHAREPOINT()
s = sharepoint.connecting()
trn = TRANSFORMATION()
dwh = DWH()
engine = dwh.engine()

#define dates
today = trn.TODAY_STR
today1 = trn.TODAY


baseurl = sharepoint.BASE_URL
r = s.get(sharepoint.SC3_IM_PM_CHGM_RF_DAILY)
jsonreturn = r.json()


def dwh_upload(dwh,tempfile,trn,code,batch_id,table,filename,engine,sheet):

    ## execute stored procedure in DWH to log the start of ETL process
    conn = dwh.connection()
    table_name = table
    start_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    dwh.execute_proc_ETL_start(conn, '[dbo].[sp_ds_StartETLProcessRun]', code, batch_id, start_time, table_name)

    for file in jsonreturn['d']['results']:
        if filename in file['Name'] and today in file['Name']:
            print('File found: {}'.format(file['Name']))
            print('')

            temp_file = '/srv/reporting/SC3/{}'.format(tempfile)

            requestedFile = s.get(baseurl+'/'+file['ServerRelativeUrl'])
            with open(temp_file, 'wb') as output:
                output.write(requestedFile.content)

            xl = pd.ExcelFile(temp_file)
            df = xl.parse(sheet, header=2, skiprows = 0)
            df = trn.dataframe_cleanup(df)

            ## remove extra blank rows created by BO in Change dataset
            if table == 'SC3_CHGM_Daily':
                df.dropna(subset=['Change_ID'], inplace = True)

            df['Load_Date'] = today
            df['Batch_ID'] = batch_id
            shape = df.shape

            ## delete any data loaded previously today
            cleanup_query = "DELETE FROM {} WHERE Load_Date = '{}'".format(table_name, today)

            with dwh.connection() as conn:
                cursor = conn.cursor()
                print('Executing query: {}'.format(cleanup_query))
                cursor.execute(cleanup_query)
                conn.commit()
                print('Done!')

            try:
                print('Uploading data into DWH table {}...'.format(table_name))


                ### Remove emoji
                if table == 'SC3_IM_Daily_IR_Activity':
                    #print('Clear Description - SC3_IM_Daily_IR_Activity')
                    df['Description']  = df['Description'].astype(str).apply(lambda x: x.encode('ascii', 'ignore').decode('ascii')) ## Delete Emoji
                if table == 'SC3_CHGM_Daily' or table == 'SC3_CHGM_Daily_Tasks':
                    #print('Clear Closure_Description - SC3_CHGM_Daily')
                    df['Closure_Description']  = df['Closure_Description'].astype(str).apply(lambda x: x.encode('ascii', 'ignore').decode('ascii')) ## Delete Emoji
                if table == 'SC3_IM_Daily' or table == 'SC3_CHGM_Daily':
                    #print('Clear Title - SC3_IM_Daily/SC3_CHGM_Daily')
                    df['Title']  = df['Title'].astype(str).apply(lambda x: x.encode('ascii', 'ignore').decode('ascii')) ## Delete Emoji
                ### Remove emoji
                df.to_sql(table_name, engine, if_exists='append',index=False)
                print('Done! Uploaded number of rows, columns: {}'.format(shape))
            except Exception as e:
                print(e)
                # execute stored procedure in DWH to log an error when uploading the data
                dwh.execute_proc_ETL_log_error(conn, '[dbo].[sp_ds_LogETLProcessError]', batch_id)
                raise ValueError('ERROR: Failed to upload data to DWH. The error has been logged. Exiting script...')

            try:
                print('Removing temp file from the server...')
                os.remove(temp_file)
                print('Done!')
            except Exception as e:
                print(e)

            ## execute stored procedure in DWH to log the end of ETL process
            end_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
            dwh.execute_proc_ETL_end(conn, '[dbo].[sp_ds_EndETLProcessRun]', batch_id, end_time, shape[0], shape[1], table_name)


def main():


    print('Looking for files that contain today\'s date: {}'.format(today))

    # check the directory if there are 4 data files with today's date
    # append the found files into a list, then check if the list contains exactly 4 files. If not, raise an error to terminate the script
    today_files = []

    for file in jsonreturn['d']['results']:
        if(today in file['Name']):
            today_files.append(file['Name'])
        else:
            pass

    if len(today_files) == 4:
        pass
    else:
        print('Files found: {}'.format(today_files))
        raise ValueError("The 4 required data files from {} were not found!".format(today))

    ## create new batch_id
    batch_id = dwh.get_batchID(engine)

    ## load all files into dwh tables
    dwh_upload(dwh,'temp_CHGM_daily.xlsx',trn,'SC3IPCRDAY',batch_id,'SC3_CHGM_Daily','SC3_CHGM_Daily',engine,'Change Records')
    dwh_upload(dwh,'temp_IM_daily.xlsx',trn,'SC3IPCRDAY',batch_id,'SC3_IM_Daily','SC3_IM_Daily',engine,'Incident Records')
    dwh_upload(dwh,'temp_PM_daily.xlsx',trn,'SC3IPCRDAY',batch_id,'SC3_PM_Daily','SC3_PM_Daily',engine,'Problem Records')
    dwh_upload(dwh,'temp_RF_daily.xlsx',trn,'SC3IPCRDAY',batch_id,'SC3_RF_Daily','SC3_RF_Daily',engine,'Request Records')
    dwh_upload(dwh,'temp_IM_daily_IR_clocks.xlsx',trn,'SC3IPCRDAY',batch_id,'SC3_IM_Daily_IR_Clocks','SC3_IM_Daily',engine,'IR - Clocks')
    dwh_upload(dwh,'temp_RF_daily_RR_clocks.xlsx',trn,'SC3IPCRDAY',batch_id,'SC3_RF_Daily_RR_Clocks','SC3_RF_Daily',engine,'RR - Clocks')
    dwh_upload(dwh,'temp_CHGM_daily_tasks.xlsx',trn,'SC3IPCRDAY',batch_id,'SC3_CHGM_Daily_Tasks','SC3_CHGM_Daily',engine,'Change Tasks')
    dwh_upload(dwh,'temp_PM_daily_PR_activity.xlsx',trn,'SC3IPCRDAY',batch_id,'SC3_PM_Daily_PR_Activity','SC3_PM_Daily',engine,'PR - Activity')
    dwh_upload(dwh,'temp_PM_daily_PR_clocks_status.xlsx',trn,'SC3IPCRDAY',batch_id,'SC3_PM_Daily_PR_Clocks_Status','SC3_PM_Daily',engine,'PR - Clocks (Status)')
    dwh_upload(dwh,'temp_IM_daily_related_IM.xlsx',trn,'SC3IPCRDAY',batch_id,'SC3_IM_Daily_Related_IM','SC3_IM_Daily',engine,'IR - Related Incidents')
    dwh_upload(dwh,'temp_PM_daily_PT_activity.xlsx',trn,'SC3IPCRDAY',batch_id,'SC3_PM_Daily_PT_Activity','SC3_PM_Daily',engine,'PT - Activity')
    dwh_upload(dwh,'temp_PM_daily_problem_tasks.xlsx',trn,'SC3IPCRDAY',batch_id,'SC3_PM_Daily_Problem_Tasks','SC3_PM_Daily',engine,'Problem Tasks')
    dwh_upload(dwh,'temp_IM_daily_IR_activity.xlsx',trn,'SC3IPCRDAY',batch_id,'SC3_IM_Daily_IR_Activity','SC3_IM_Daily',engine,'IR - Activity')
    dwh_upload(dwh,'temp_CHGM_daily_CR_impacted_CIs.xlsx',trn,'SC3IPCRDAY',batch_id,'SC3_CHGM_Daily_CR_Impacted_CIs','SC3_CHGM_Daily',engine,'CR - Impacted CIs')

    print('*****************************************************')


if __name__ == '__main__':
    main()
