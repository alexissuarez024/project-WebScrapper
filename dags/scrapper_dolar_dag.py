"""""""""
Import of the utils from Airflow
"""""""""
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

"""""""""
Import of script utilities
"""""""""
import requests as rq
from bs4 import BeautifulSoup as bs
import psycopg2
from config_connection_dag import Connection
from config_scrapper_dag import webScrapper
from zona_horaria_dag import rosario_date
import os

"""""""""
Instance of global variable
"""""""""
scrapper = webScrapper()

response = rq.get(scrapper.dataDolar())
soup = bs(response.text, 'html.parser')
db = Connection(psycopg2)

"""""""""
10) Lastly, one last check to find duplicate data and delete it as done above.
"""""""""
def refreshData():
    try:
        response = db.curFetchAll("""
            with c as (
                select * from 
                                ( select *, COUNT(*) over (partition by date_actualization,buy_values,sell_values order by id) as duplicado
                                from scrapper_dolar_v1
                                ) as A
                where duplicado > 1 
            ) select id,case 
                when duplicado > 1 then 'repeat' 
            end flag
            from c
        
            """
            )

        print(response)
        print(type(response))
        
        if response == []:
            print("There's no answer")

        else:      
            for res in response:
                i = 0
                print(res)
                if str(res[1]) == 'repeat':
                    print('ITERABLE',res[i])
                    print('Equal')
                    db.curExecute(f"""
                        DELETE FROM scrapper_dolar_v1 WHERE id = {res[i]}
                    """)
                    i = i + 1

                else: 
                    print('Dont equal')

    except Exception as e:
        print(e)


"""""""""
9) Function to write inside the data that we have in the database
"""""""""
def writeDolarData():
    try:
        today = rosario_date
        i = 0
        responseDolar = db.curFetchAll(f"""
            select * from scrapper_dolar_v1;
        """)

        
        for res in responseDolar:
            resId = res[0]
            resDolarType = res[1]
            resBuyValues = res[2]
            resSellValues = res[3]
            resPercentage = res[4]
            resDate = res[5]
            resUser = res[6]

            with open(f'dolar_folders_{today}/folder_{today}.csv', 'a', encoding='utf-8') as f:
                f.write(f"{resId};{resDolarType};{resBuyValues};{resSellValues};{resPercentage};{resDate};{resUser}\n")
            
        print(len(responseDolar))

    except Exception as e:
        print(e)


"""""""""
8) In this step we create a folder to save the data
"""""""""
def dataFolder():
    try:
        #Importing the current date variable
        today = rosario_date
        #In the event that the directory is not found in the indicated path, we create it
        if not os.path.isdir(f"dolar_folders_{today}"):
                os.mkdir(f"dolar_folders_{today}")
                #And inside we writing the next
                with open(f'dolar_folders_{today}/folder_{today}.csv', 'w', encoding='utf-8') as f:
                    f.writelines("id;dolar_types;buy_values;sell_values;percentage;date_actualization;updated_by")
                    f.writelines("\n")

        #If the directory is already found in the selected path, then we just open it and type the following
        else:
            with open(f'dolar_folders_{today}/folder_{today}.csv', 'w', encoding='utf-8') as f:
                f.writelines("id;dolar_types;buy_values;sell_values;percentage;date_actualization;updated_by")
                f.writelines("\n")    
    
    except Exception as e:
        print(e)


"""""""""
7) Function of insertion data in case havent repeat 
"""""""""
def load_buy(dolar,buy_values,sell_values,percentage,date_actualization):
    try:
        user = 'airflow'
        w = 0
        i = 0
        while w <= len(dolar):
            if i <= len(dolar):
                w += 1
                print('LOAD_BUY',dolar[i],'Compra',buy_values[i],'Venta', sell_values[i],percentage[i],date_actualization[i], user)

                db.curExecute(f"""
                    INSERT INTO scrapper_dolar_v1 (dolar_types, buy_values,sell_values,percentage,date_actualization, updated_by)
                    VALUES('{dolar[i]}',{float(buy_values[i])},{float(sell_values[i])},'{percentage[i]}','{date_actualization[i]}', '{user}');

                    """)

                i += 1
            
            else:
                print('Stop')
                break

    except Exception as e:
        print('End')    


"""""""""
6) In this step, we check not data repeat inside database
   If the data is repeated, then we delete it. Else we insert the data 
"""""""""
def if_insert(dolar,buy_values,sell_values,percentage,date_actualization):
    try:
        #Query the database to find duplicate data and intercept it
        response = db.curFetchAll("""
            with c as (
                select * from 
                                ( select *, COUNT(*) over (partition by date_actualization,buy_values,sell_values order by id) as duplicado
                                from scrapper_dolar_v1
                                ) as A
                where duplicado > 1 
            ) select id,case 
                when duplicado > 1 then 'repeat' 
            end flag
            from c
        
            """
            )
        #Printing the query response
        print(response)
        #And its type
        print(type(response))
        
        #In the event that the response is empty, we insert the data through the following function 
        if response == []:
            print("There's no answer")
            load_buy(dolar,buy_values,sell_values,percentage,date_actualization)

        else:
            # Otherwise, if the data has content, we iterate and extract the id to remove it      
            for res in response:
                i = 0
                print(res)
                if str(res[1]) == 'repeat':
                    print('ITERABLE',res[i])
                    print('Equal')
                    db.curExecute(f"""
                        DELETE FROM scrapper_dolar_v1 WHERE id = {res[i]}
                    """)
                    i = i + 1

                else: 
                    print('Dont equal')

    except Exception as e:
        print(e)


"""""""""
5) Extraction of the last date actualization the dolar
"""""""""
def date_actualization(dolar,buy_values,sell_values,percentage):
    try:
        soup_date = soup.find_all("td", class_="date")
        date_actualization = []
        for date in soup_date:
            transform_date = str(date.text).replace("Actualizado: ","").replace(".","-")
            date_actualization.append(transform_date)
        
        #This here, just is for check data
        print('RECIBIDO --> ',dolar)   
        print('RECIBIDO --> ',buy_values)
        print('RECIBIDO --> ',sell_values)
        print('RECIBIDO --> ',percentage)
        print(date_actualization)
        #Sending after data and the new data extract to the next function
        if_insert(dolar,buy_values,sell_values,percentage,date_actualization)
    
    except Exception as e:
        print(e)


"""""""""
4) Extraction of the percentage of rise or fall of the dollar
"""""""""
def percentage(dolar,buy_values,sell_values):
    try:
        #Pull data of the tags 'td' with class 'percentage'
        soup_percentage = soup.find_all('td', class_="percentage")
        percentage = []
        for p in soup_percentage:
            #For each one, we extract and transform its content
            transform_percentage = str(p.text).replace("%","")
            percentage.append(transform_percentage)

        print(percentage)
        print('RECIBIDO --> ',dolar)
        print('RECIBIDO --> ',buy_values)
        print('RECIBIDO --> ',sell_values)
        #Sending after data and the new data extract to the next function
        date_actualization(dolar,buy_values,sell_values,percentage)   
    
    except Exception as e:
        print(e)


"""""""""
3) Extraction of the value of the dollar for the purchase and sale
"""""""""
def buy_and_sell_values(dolar):
    try:
        #Pull data of the tags 'div' with class 'buy-value' and 'sell-value'        
        soup_buy_values = soup.find_all('div', class_="buy-value")
        soup_sell_values = soup.find_all('div', class_="sell-value")
        buy_values = []
        sell_values = []
        
        #If we have data empty, we send an impression with the information of what happened
        if soup_buy_values == []:
            print('The website does not work, there not data BUY_VALUES')
        else:
            for buy in soup_buy_values:
                #Else, we extract and transform the content 
                transform_buy = str(buy.text).replace('$',"").replace(",",".")
                buy_values.append(transform_buy)

        if soup_sell_values == []:
            print('The website does not work, there not data SELL_VALUES')
        else:                
            for sell in soup_sell_values:
                transform_sell = str(sell.text).replace("$","").replace(",",".")
                sell_values.append(transform_sell)

        #Here, we remplace a value empty for '0.00'
        buy_values.insert(2,'0.00')
        print('BUY VALUES ->',buy_values)
        print('SELL VALUES ->',sell_values)
        print('RECIBIDO --> ', dolar)
        #Sending after data and the new data extract to the next function
        percentage(dolar,buy_values,sell_values)

    except Exception as e:
        print(e)


"""""""""
2) Scrapping the page for extract data about of the dolar price and comparate with price in Argentina.
   In this function, we extract the dollar rate 
"""""""""
def type_dolar():
    try:
        #Pulling all tags 'td' with 'class = name' 
        dolar_type = soup.find_all('td', class_="name")
        dolar = []
        for d in dolar_type:
            #For each one, we extract its content 
            dolar.append(d.text)
            
        print(dolar)
        
        #Sending data extraction to the next function
        buy_and_sell_values(dolar)
    except Exception as e:
        print(e)


"""""""""
1) First the creation of the table in the database.
"""""""""
def create_table():
    try:
        db.curExecute("""
            CREATE TABLE IF NOT EXISTS scrapper_dolar_v1 (
                id int8 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
                dolar_types VARCHAR(100),
                buy_values float8,
                sell_values float8,
                percentage VARCHAR(20),
                date_actualization VARCHAR(100),
                updated_by VARCHAR(50)
            )
            """
            )
        print('Table created')

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


"""""""""
In this sector we find the tasks to direct them with airflow
"""""""""
default_args = {
    'owner' : 'alexis', 
    'dependes_on_past' : False, 
    'email' : ['airflow'], 
    'email_on_failure' : False, 
    'email_on_retry' : False,
    'retries' : 1, 
    'retry_delay' : timedelta(minutes=5), 
}


with DAG(
    'scrapper_dolar_v1', 
    default_args = default_args,
    schedule_interval = '*/15 12-23 * * 1-5', 
    start_date = days_ago(2), 
    tags = ['scrapper_v1'], 

) as dag: 

    # We perform several inserts to ensure that we extract the data in its entirety
    # That is why three insertion tasks are shown
    insert_data_table_task_r1 = PythonOperator(task_id = 'Insert_data_table1', python_callable = type_dolar)
    insert_data_table_task_r2 = PythonOperator(task_id = 'Insert_data_table2', python_callable = type_dolar)
    insert_data_table_task_r3 = PythonOperator(task_id = 'Insert_data_table3', python_callable = type_dolar)
    # We verify that there are no duplicates
    refresh_data_task = PythonOperator(task_id = 'Refresh_data_for_repeat', python_callable = refreshData)
    # We create the files to send the csv
    created_data_folder = PythonOperator(task_id = 'Creation_of_the_data_folder', python_callable = dataFolder)
    write_in_data_folder = PythonOperator(task_id = 'Writing_inside_folder_data', python_callable = writeDolarData)

    insert_data_table_task_r1 >> insert_data_table_task_r2 >> insert_data_table_task_r3 >> refresh_data_task >> write_in_data_folder
    created_data_folder
