# %%

import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import time
from dateutil.relativedelta import relativedelta
import arrow
import re
from pathlib import Path

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)




try :


    ##--------------------------------------------------------------------------------
    defilename = "wh_mc_material_intake"
    dename = "Sirilack and Amitita"
    # --------------------------------------------------
    # -------------------------------------
    project_name_log = 'wh_mc_material_intake'
    destinationsql = 'wh_mc_material_intake'
    # ---------------------------------
    ##---------------------------------------------------------------------------------


    Path("./log/").mkdir(parents=True, exist_ok=True)




    start_run_time = time.time()


    ##Extract-------------------------------------------------------------------------------------------------

    ##--------------------------------------------------------------------------------------------------------
    ##--------------------------------------------------------------------------------------------------------
    server = '12.1.2.41'
    database = '03PRD_Warehouse'
    username = 'powerbi'
    password = 'Svi14002*'

    # Establish the connection
    conn = pyodbc.connect(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    )
    cursor = conn.cursor()



    run_mode = 'normal run' 
    # run_mode = 'force run'

    if run_mode == 'normal run' :

        current_date = datetime.now() - timedelta(0)
        tomorro_date = current_date + timedelta(1)
        current_week = current_date.isocalendar()[1]
        current_quarter = (current_date.month - 1) // 3 + 1

        yr  = current_date.year
        mth = current_date.month
        day = current_date.day

        current_date_txt = f"{yr}-{str(mth).zfill(2)}-{str(day).zfill(2)}"


        list_date_to_process = [current_date_txt]

    elif run_mode == 'force run' :

        list_date_to_process = ['2025-01-31','2025-02-01','2025-02-03','2025-02-04','2025-02-05','2025-02-06']


    file = open(f'log/{project_name_log}.txt','a')
    file.write(f"Start process " + str(arrow.now().format('DD-MM-YYYY HH:mm:ss'))+"\n")
    file.write(f"process_date={list_date_to_process}\n")
    file.close()


    for date_to_process in list_date_to_process :



        query = f'''
        SELECT
            zr.plant,
            zr.days,
            zr.pur_grp,
            zr.pur_name,
            zr.mat_grp,
            zr.material,
            zr.mat_description,
            zr.po_no,
            zr.ln_,
            zr.open_qty,
            zr.conf_qty,
            zr.conf_date,
            zr.revise_date,
            zr.act,
            zr.supplier_code,
            zr.manufacturer,
            zr.aging_po,
            zr.intransit_inv,
            zr.intransit_qty,
            zr.traffic_job_no,
            zr.pdt,
            zr.schedule_ln,
            zr.mpn_part_po,
            zr.inbound_no,
            zr.unit,
            zr.unit_price,
            zr.per,
            zr.curr,
            zr.supplier,
            zr.supplier_item,
            zr.createtime,
            zr.cancellation,
            zr.supplier_remark,
            zr.reschedule_window,
            zr.unit_price_usd_,
            zr.amount_usd_,
            zr.buyer_comment,
            zr.item_ack,
            zr.svi_req_date,
            zr.order_date,
            zr.email_address,
            LEFT(zr.material, 3) AS cus,
            bu.bu,
            bu.cft   
        FROM wh_zrmm0010_daily zr
        LEFT JOIN wh_cust_and_bu_updated bu
        ON LEFT(zr.material, 3) = bu.cust
        AND 
            CASE 
                WHEN zr.plant IN ('SVI2', 'TS1') THEN 'TH'
                WHEN zr.plant IN ('SEC1', 'SEC2') THEN 'AEC'
                ELSE zr.plant
            END = bu.plant
        WHERE CONVERT(DATE, createtime) = '{date_to_process}'  -- CONVERT(DATE, GETDATE())
        '''


        df = pd.read_sql(query, conn)
        records = len(df)
        # print(f'Read input row={records}')

        df_ori = df.copy()




        # สร้างคอลัมน์ bu_cft, wk และ yr
        df['bu_cft'] = df['bu'] + ' - ' + df['cft'] 
        df['wk'] = df['createtime'].dt.isocalendar().week # 'W' + df['createtime'].dt.isocalendar().week.astype(str).str.zfill(2)
        df['yr'] = df['createtime'].dt.isocalendar().year


        # จัดการ conf_date
        df['conf_date_cal'] = pd.to_datetime(df['conf_date'], errors='coerce')
        df['year_conf'] = df['conf_date_cal'].dt.year.astype('Int64')  # ใช้ pd.Int64Dtype()
        df['quarter_conf'] = ((df['conf_date_cal'].dt.month - 1) // 3 + 1).astype('Int64')
        df['ww_conf'] = df['conf_date_cal'].dt.isocalendar().week
        df['quarter_conf_date'] = np.where(
            df['conf_date_cal'].notna(),
            df['year_conf'].astype(str) + 'Q' + df['quarter_conf'].astype(str),
            None
        )

        # จัดการ revise_date
        df['revise_date_cal'] = pd.to_datetime(df['revise_date'], errors='coerce')
        df['year_revise'] = df['revise_date_cal'].dt.year.astype('Int64')  # ใช้ pd.Int64Dtype()
        df['quarter_revise'] = ((df['revise_date_cal'].dt.month - 1) // 3 + 1).astype('Int64')
        df['ww_revise'] = df['revise_date_cal'].dt.isocalendar().week
        df['quarter_revise_date'] = np.where(
            df['revise_date_cal'].notna(),
            df['year_revise'].astype(str) + 'Q' + df['quarter_revise'].astype(str),
            None
        )
        df


        def get_last_date_of_current_quarter():
            # Get the current date
            today = date.today()
            # Determine the current quarter based on the month
            current_month = today.month
            if current_month in (1, 2, 3):  # Q1
                last_date = date(today.year, 3, 31)
            elif current_month in (4, 5, 6):  # Q2
                last_date = date(today.year, 6, 30)
            elif current_month in (7, 8, 9):  # Q3
                last_date = date(today.year, 9, 30)
            else:  # Q4
                last_date = date(today.year, 12, 31)
            return last_date

        def get_last_date_of_current_week():
            today = date.today()
            # Calculate the number of days to add to get to Sunday
            days_to_sunday = 6 - today.weekday()  # Monday is 0, Sunday is 6
            sunday = today + timedelta(days=days_to_sunday)
            return sunday


        ############################################################################################
        # สร้างคอลัมน์ action_status_of_po
        def get_action_status_of_po(row):

            # If date is blank or '00/00/000'
            if not re.search( "datetime" , str( type(row['revise_date']) ) ) : # str( type(row['revise_date']) ) != "<class 'datetime.date'>" :
                return 'Cancel order'
            
            # If(Qtr of Revise Date != blank) and
            # ►if(Conf/ Date>Current Qtr and year)  
            elif re.search( "datetime" , str( type(row['revise_date']) ) ) and not re.search( "datetime" , str( type(row['conf_date']) ) ) : # (row['revise_date'] is not blank) and (row['conf_date'] is blank) :
                return "PO no confirm"
            
            elif (row['revise_date'] <= last_date_of_curr_qtr) and (row['conf_date'] <= last_date_of_curr_qtr) :
                return 'PO Cfm ok for need date in Qtr'

            # If(Qtr of Revise Date <= Current Qtr and year) and
            # ►if(Conf/ Date > Current Qtr and year)  
            elif (row['revise_date'] <= last_date_of_curr_qtr) and (row['conf_date'] > last_date_of_curr_qtr) :
                return 'Pull in PO out of Qtr'

            # If(Qtr of Revise Date > Current Qtr and year) and
            # ►if(Conf/ Date<=Current Qtr and year)  
            elif (row['revise_date'] > last_date_of_curr_qtr) and (row['conf_date'] <= last_date_of_curr_qtr) :
                return 'Push out PO no need in Qtr'
            
        last_date_of_curr_qtr = get_last_date_of_current_quarter()

        df['action_status_of_po'] = df.apply(get_action_status_of_po, axis=1)
        # print(f'created "action_status_of_po" column done.')


        ############################################################################################
        # สร้างคอลัมน์ materials_intake_by_week
        def get_materials_intake_by_week(row) :


            date_cal = row['conf_date']

            # If date not blank
            if re.search( "datetime" , str( type(row['revise_date'])))  and re.search( "datetime" , str(type(row['conf_date']))) :
                
                # In case : 'Pull in PO out of Qtr'
                if (row['revise_date'] <= last_date_of_curr_qtr) and (row['conf_date'] > last_date_of_curr_qtr) :
                    date_cal = row['revise_date']

            # If date not blank
            if re.search( "datetime" , str( type(date_cal) ) ) :

                # Calculate week, quater and year of its date
                wk  = date_cal.isocalendar()[1]
                qtr = (date_cal.month - 1) // 3 + 1
                yr  = date_cal.strftime('%Y') 

                # Find range of week to set for 'Materials intake by week'
                if date_cal <= last_date_of_curr_week : 
                    return f"W{str(row['wk']).zfill(2)}"
                elif date_cal <= last_date_of_curr_qtr :
                    return f'W{str(wk).zfill(2)}'

        last_date_of_curr_week = get_last_date_of_current_week()

        df['materials_intake_by_week'] = df.apply(get_materials_intake_by_week, axis=1)
        # print(f'created "materials_intake_by_week" column done.')


        # สร้างคอลัมน์ requirement, total_materials_intake, materials_intake_over_requirement
        df['requirement'] = df['amount_usd_']
        df['total_materials_intake'] = df['conf_qty'] * df['unit_price_usd_']
        df['materials_intake_over_requirement'] = df['total_materials_intake'] - df['requirement']


        ############################################################################################
        ## This is 'in_active' customer by condition below:
        # customer is 'SMILE CONTAINER'
        # po_no start with '550000'

        def get_is_active(row):
            if str(row['po_no']).startswith('550000'):
                return "inactive"
            elif str(row['manufacturer']) == 'SMILE CONTAINER' :
                return "inactive"
            else:
                return "active"
        df['is_active'] = df.apply(get_is_active, axis=1)
        # print(f'created "is_active" column done.')


        ############################################################################################
        # Convert USD to THB
        # - columns 'amount_usd'  * exchange_rate(buying_transfer[USD])   
        # - เปลี่ยนชื่อ amount_usd  เป็น  total_material_intake_usd, total_material_intake_thb
        # - ดึง exchange_rate วันจันทร์เท่านั้น 



        # step
        # 1. unique date that existing in df['createtime'] 

        df['wk'] = df['createtime'].dt.isocalendar().week # 'W' + df['createtime'].dt.isocalendar().week.astype(str).str.zfill(2)
        df['yr'] = df['createtime'].dt.isocalendar().year

        


        # 2. get exchange rate from Monday of its week following timestamp of column 'createtime'

        def get_monday_of_week(dt):
            # today = datetime.today()
            today = dt.astype('datetime64[ms]').astype(datetime) # .to_pydatetime()
            # Calculate the number of days to subtract to get to Monday
            days_to_monday = today.weekday()  # Monday is 0, Sunday is 6
            monday = today - timedelta(days=days_to_monday)

            return f"{monday.year}-{str(monday.month).zfill(2)}-{str(monday.day).zfill(2)}"


        def get_currency(data):

            # unique create time to list for getting currency  
            list_createtime = df['createtime'].unique()

            for ct in list_createtime :

                # print('createtime=',ct)
                monday_date = get_monday_of_week(ct)

                # Get exchange rate from Monday only
                query = f''' SELECT buying_transfer FROM STG.dbo.stg_exchange_rate_all WHERE period = '{monday_date}' and currency = 'USD' '''
                # print(query)
                cursor.execute(query)
                buying_transfer_rate_usd = float(cursor.fetchone()[0])

                data.loc[data['createtime'] == ct, 'exchange_rate'] = buying_transfer_rate_usd

                return data
            

        
        df = get_currency(df) # return input dataframe plus 'exchange_rate' column

        df['total_material_intake_thb'] = df['amount_usd_'] * df['exchange_rate']
        df.rename(columns={'amount_usd_': 'total_material_intake_usd'}, inplace=True)
        # print(f'created "total_material_intake_thb" column done.')



        ############################################################################
        ## Ingest to DB
        print("Start ingest to DB")
        # file = open(f'log/{project_name_log}.txt','a')
        # file.write(f"start ingest to DB " + str(arrow.now().format('DD-MM-YYYY HH:mm:ss'))+"\n")
        # file.close()

        ft_time_process = start_run_time - time.time()
        st_time_ingest  = time.time()


        #string transform
        def string_col(col_name):
            df[f'{col_name}'] = np.where((df[f'{col_name}'].isna()) | (df[f'{col_name}'] == ''), 'NULL', df[f'{col_name}'].astype(str).str.replace("'", "''").apply(lambda x: f"'{x}'"))
            
        string_cols = ['plant','days','pur_grp','pur_name','mat_grp','material','mat_description','po_no','act','supplier_code','manufacturer','intransit_inv','intransit_qty','traffic_job_no','mpn_part_po','inbound_no','unit','curr','supplier','supplier_item','cancellation','supplier_remark','buyer_comment','item_ack','email_address','cus','bu','bu_cft','quarter_conf_date','quarter_revise_date','action_status_of_po','materials_intake_by_week','is_active']
        for col_name in string_cols:
            string_col(col_name)

        #int transform
        def integer_col(col_name):
            df[f'{col_name}'] = df[f'{col_name}'].fillna('NULL')

        interger_cols = ['ln_','open_qty','conf_qty','aging_po','pdt','schedule_ln','per','wk','yr']
        for col_name in interger_cols:
            integer_col(col_name)

        # Float transform
        def float_col(col_name):
                df[f'{col_name}'] = df[f'{col_name}'].astype(str)
                df[f'{col_name}'] = df[f'{col_name}'].str.strip()
                df[f'{col_name}'] = np.where(df[f'{col_name}'].str.endswith('-'), df[f'{col_name}'].str.rstrip('-').apply(lambda x: f"-{x}"), df[f'{col_name}'])
                df[f'{col_name}'] = df[f'{col_name}'].replace(',', '', regex=True)
                # df[f'{col_name}'] = df[f'{col_name}'].astype(float)
                df[f'{col_name}'] = pd.to_numeric(df[f'{col_name}'], errors='coerce')
                df[f'{col_name}'] = df[f'{col_name}'].fillna('NULL')

        float_cols = ['reschedule_window','unit_price_usd_','unit_price','total_material_intake_usd','total_material_intake_thb','total_materials_intake','requirement','materials_intake_over_requirement','exchange_rate']
        for col_name in float_cols:
            float_col(col_name)

        #date transform
        def date_col(col_name):

            # Extract timestamp with '2025-02-03 00:00:00.000'
            pattern = r"^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d{3})"
            extract_datetime = lambda s: re.search(pattern, str(s))[0] if re.search(pattern, str(s)) is not None else np.nan
            df[f'{col_name}'] = df[f'{col_name}'].apply(extract_datetime)

            df[f'{col_name}'] = np.where(df[f'{col_name}'].isna(), 'NULL', df[f'{col_name}'].apply(lambda x: f"'{x}'"))

        date_cols = ['conf_date','revise_date','createtime','svi_req_date','order_date']
        for col_name in date_cols:
            date_col(col_name)
        ##Load-------------------------------------------------------------------------------------------------

        del_query = f''' DELETE from {destinationsql} where CONVERT(DATE, createtime) = '{date_to_process}' '''
        # print(del_query)
        cursor.execute(del_query)
        


        count = 0
        for idx1, item1 in df.iterrows():
            query_insert = f'''insert into {destinationsql} (
                                    wk,
                                    yr,
                                    plant,
                                    days,
                                    pur_grp,
                                    pur_name,
                                    mat_grp,
                                    material,
                                    mat_description,
                                    po_no,
                                    ln_,
                                    open_qty,
                                    conf_qty,
                                    conf_date,
                                    revise_date,
                                    act,
                                    supplier_code,
                                    manufacturer,
                                    aging_po,
                                    intransit_inv,
                                    intransit_qty,
                                    traffic_job_no,
                                    pdt,
                                    schedule_ln,
                                    mpn_part_po,
                                    inbound_no,
                                    unit,
                                    unit_price,
                                    per,
                                    curr,
                                    supplier,
                                    supplier_item,
                                    createtime,
                                    cancellation,
                                    supplier_remark,
                                    reschedule_window,
                                    unit_price_usd_,
                                    buyer_comment,
                                    item_ack,
                                    svi_req_date,
                                    order_date,
                                    email_address,
                                    customer,
                                    bu,
                                    bu_cft,
                                    quarter_conf_date,
                                    quarter_revise_date,
                                    action_status_of_po,
                                    materials_intake_by_week,
                                    total_materials_intake,
                                    requirement,
                                    materials_intake_over_requirement,
                                    total_material_intake_usd,
                                    total_material_intake_thb,
                                    exchange_rate,
                                    is_active

                                )
                                values(
                                    {item1['wk']},
                                    {item1['yr']},
                                    {item1['plant']},
                                    {item1['days']},
                                    {item1['pur_grp']},
                                    {item1['pur_name']},
                                    {item1['mat_grp']},
                                    {item1['material']},
                                    {item1['mat_description']},
                                    {item1['po_no']},
                                    {item1['ln_']},
                                    {item1['open_qty']},
                                    {item1['conf_qty']},
                                    {item1['conf_date']},
                                    {item1['revise_date']},
                                    {item1['act']},
                                    {item1['supplier_code']},
                                    {item1['manufacturer']},
                                    {item1['aging_po']},
                                    {item1['intransit_inv']},
                                    {item1['intransit_qty']},
                                    {item1['traffic_job_no']},
                                    {item1['pdt']},
                                    {item1['schedule_ln']},
                                    {item1['mpn_part_po']},
                                    {item1['inbound_no']},
                                    {item1['unit']},
                                    {item1['unit_price']},
                                    {item1['per']},
                                    {item1['curr']},
                                    {item1['supplier']},
                                    {item1['supplier_item']},
                                    {item1['createtime']},
                                    {item1['cancellation']},
                                    {item1['supplier_remark']},
                                    {item1['reschedule_window']},
                                    {item1['unit_price_usd_']},
                                    {item1['buyer_comment']},
                                    {item1['item_ack']},
                                    {item1['svi_req_date']},
                                    {item1['order_date']},
                                    {item1['email_address']},
                                    {item1['cus']},
                                    {item1['bu']},
                                    {item1['bu_cft']},
                                    {item1['quarter_conf_date']},
                                    {item1['quarter_revise_date']},
                                    {item1['action_status_of_po']},
                                    {item1['materials_intake_by_week']},
                                    {item1['total_materials_intake']},
                                    {item1['requirement']},
                                    {item1['materials_intake_over_requirement']},
                                    {item1['total_material_intake_usd']},
                                    {item1['total_material_intake_thb']},
                                    {item1['exchange_rate']},
                                    {item1['is_active']}


                                )'''
            # print(query_insert)
            cursor.execute(query_insert)

            count+=1
            if count % 5000 == 0:
                cursor.commit()
                count = 0

            progress_message = f"Process insert SQL: {idx1 + 1}/{len(df)}"
            clear_spaces = " " * len(progress_message)
            print(f"\r{clear_spaces}\r{progress_message}", end="", flush=True)
            
        cursor.commit()
        print('load successful')
        ft_time_ingest  = st_time_ingest - time.time()


        file = open(f'log/{project_name_log}.txt','a')
        file.write(f"Load {records} records success; time_process= {ft_time_process/ 60} minute, time_ingest={ft_time_ingest/60} minute.\n")
        file.write("Done processing "+ str(arrow.now().format('DD-MM-YYYY HH:mm:ss'))+"\n")
        file.close()


except Exception as e:

    file = open('log/wh_pl_sum.txt','a')
    file.write(f"Load fail at date={list_date_to_process} with error:" +str(e)+ " \n")
    file.close()










