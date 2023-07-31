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
import lxml.html as html
import psycopg2
from bs4 import BeautifulSoup as bs
from config_connection_dag import Connection
from config_scrapper_dag import webScrapper
from zona_horaria_dag import date_now


"""""""""
Instance of global variable
"""""""""
scrapper = webScrapper()

db = Connection(psycopg2)


"""
It's just a function that you don't have to use, but in script development, it's good to have a rough draft handy.
"""
def dropTables():
    try:

        print("Also delete dw?")
        print("YES or NO = [y/n]")
        responseDW = str(input()).lower()

        if responseDW == 'y':
            db.curExecute("""
                DROP TABLE IF EXISTS staging_musimundo_samsung;
                DROP TABLE IF EXISTS staging_personal_samsung;
                DROP TABLE IF EXISTS staging_movistar_samsung;
                DROP TABLE IF EXISTS int_samsung;
                DROP TABLE IF EXISTS dw_samsung;
            """)

        else:
            db.curExecute("""
                DROP TABLE IF EXISTS staging_musimundo_samsung;
                DROP TABLE IF EXISTS staging_personal_samsung;
                DROP TABLE IF EXISTS staging_movistar_samsung;
                DROP TABLE IF EXISTS int_samsung;
            """)


    except Exception as e:
        print(e)



""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
"""
Sector to delete duplicate data from the different staging tables
"""
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
def deleteRepeatMovistar():
    try:
        responseMovistar = db.curFetchAll("""
            with c as (
                select * from 
                                ( select *, COUNT(*) over (partition by name_cel,price order by id) as duplicado
                                from staging_movistar_samsung
                                ) as A
                where duplicado > 1 
            ) select id,case 
                when duplicado > 1 then 'repeat' 
            end flag
            from c
        
            """)
        
        print(responseMovistar)
        print(type(responseMovistar))

        for resMovi in responseMovistar:
                r = 0
                print(resMovi)
                print('There are repeated')
                if str(resMovi[1]) == 'repeat':
                    print('ITERABLE',resMovi[r])
                    print('Equal')
                    db.curExecute(f"""
                        DELETE FROM staging_movistar_samsung WHERE id = {resMovi[r]}
                    """)
                    r = r + 1

                else: 
                    print('Dont equal')

    except Exception as e:
        print(e)    


def deleteRepeatPersonal():
    try:
        responsePersonal = db.curFetchAll("""
            with c as (
                select * from 
                                ( select *, COUNT(*) over (partition by name_cel,price order by id) as duplicado
                                from staging_personal_samsung
                                ) as A
                where duplicado > 1 
            ) select id,case 
                when duplicado > 1 then 'repeat' 
            end flag
            from c
        
            """)
        
        print(responsePersonal)
        print(type(responsePersonal))

        for resPer in responsePersonal:
                r = 0
                print(resPer)
                print('There are repeated')
                if str(resPer[1]) == 'repeat':
                    print('ITERABLE',resPer[r])
                    print('Equal')
                    db.curExecute(f"""
                        DELETE FROM staging_personal_samsung WHERE id = {resPer[r]}
                    """)
                    r = r + 1

                else: 
                    print('Dont equal')

    except Exception as e:
        print(e)    


def deleteRepeatMusimundo():
    try:
        responseMusi = db.curFetchAll("""
            with c as (
                select * from 
                                ( select *, COUNT(*) over (partition by name_cel,price order by id) as duplicado
                                from STAGING_musimundo_samsung
                                ) as A
                where duplicado > 1 
            ) select id,case 
                when duplicado > 1 then 'repeat' 
            end flag
            from c
        
            """)
        
        print(responseMusi)
        print(type(responseMusi))

        for resMusi in responseMusi:
                r = 0
                print(resMusi)
                print('There are repeated')
                if str(resMusi[1]) == 'repeat':
                    print('ITERABLE',resMusi[r])
                    print('Equal')
                    db.curExecute(f"""
                        DELETE FROM STAGING_musimundo_samsung WHERE id = {resMusi[r]}
                    """)
                    r = r + 1

                else: 
                    print('Dont equal')

    except Exception as e:
        print(e)    
    
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""


""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
"""
These functions correspond to the extraction, detection of duplicates and insertion of the data.
"""
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
"""
The "union" functions are for detecting duplicates. 
If it doesn't find them, it will insert them.
"""

def union_movistar(titles,prices):
    try:
        i = 0
        print('\n')
        print('Movistar shop, samsungs ofert\n')
        response = db.curFetchAll("""
            with c as (
                select * from 
                                ( select *, COUNT(*) over (partition by name_cel order by price) as duplicado
                                from staging_movistar_samsung
                                ) as A
                where duplicado > 1 
            ) select id,case 
                when duplicado > 1 then 'repeat' 
            end flag
            from c
        
            """)
        
        print(response)
        print(type(response))

        if response == []:
            print("There's no answer")

            while i < len(titles):
                print(f"Samsung phone {titles[i]} a {float(prices[i])}")

                db.curExecute(f"""
                    INSERT INTO staging_movistar_samsung(name_cel, price, date)
                    VALUES('{titles[i]}',{float(prices[i])}, '{date_now}')
                
                """)

                i = i + 1
        else:
            print('There are repeated')

        
    except Exception as e:
        print(e)


def union_personal(titles,prices):
    try:
        i = 0
        print('\n')
        print('Personal shop, samsungs ofert\n')
        response = db.curFetchAll("""
            with c as (
                select * from 
                                ( select *, COUNT(*) over (partition by name_cel order by price) as duplicado
                                from staging_personal_samsung
                                ) as A
                where duplicado > 1 
            ) select id,case 
                when duplicado > 1 then 'repeat' 
            end flag
            from c
        
            """)
        
        print(response)
        print(type(response))

        if response == []:
            print("There's no answer")
            while i < len(titles):
                print(f"{titles[i]} a {prices[i]}")

                db.curExecute(f"""
                    INSERT INTO staging_personal_samsung(name_cel, price, date)
                    VALUES('{titles[i]}',{float(prices[i])}, '{date_now}')
                
                """)

                i = i + 1
        
        else:
            print('There are repeated')

    except Exception as e:
        print(e)


def union_musimundo(titles,prices):
    try:
        i = 0
        print('\n')
        print('Musimundo shop, samgungs ofert\n')
        response = db.curFetchAll("""
            with c as (
                select * from 
                                ( select *, COUNT(*) over (partition by name_cel order by price) as duplicado
                                from staging_musimundo_samsung
                                ) as A
                where duplicado > 1 
            ) select id,case 
                when duplicado > 1 then 'repeat' 
            end flag
            from c
        
            """)
        
        print(response)
        print(type(response))

        if response == []:
            print("There's no answer")
            while i < len(titles):
                print(f"Phone {titles[i]} a {float(prices[i])}")
                
                
                db.curExecute(f"""
                    INSERT INTO staging_musimundo_samsung(name_cel, price, date)
                    VALUES('{titles[i]}',{float(prices[i])}, '{date_now}')
                
                """)
                i = i + 1
        else:
            print('There are repeated')

    except Exception as e:
        print(e)

"""
The "samsung" functions are responsible for extracting and transforming the data
"""
def movistar_samsung():
    try:
        response_prices = rq.get(scrapper.dataMovi()['urlMovi'])
        if response_prices.status_code == 200:
            home = response_prices.content.decode('utf-8')
            parsed = html.fromstring(home)
            titles = parsed.xpath(scrapper.dataMovi()['titleMovi'])
            prices = parsed.xpath(scrapper.dataMovi()['priceMovi'])
            container_titles = []
            container_prices = []
            
            for title in titles:
                container_titles.append(title)
            
            for price in prices:
                new_price = price.replace("$","").replace(",00","").replace(".","")
                container_prices.append(float(new_price))
        
        union_movistar(container_titles,container_prices)
        
    except Exception as e:
        print(e)


def personal_samsung():
    try:
        response = rq.get(scrapper.dataPer()['urlPer'])
        if response.status_code == 200:
            home = response.content.decode('utf-8')
            parsed = html.fromstring(home)
            titles = parsed.xpath(scrapper.dataPer()['titlePer'])
            prices = parsed.xpath(scrapper.dataPer()['pricePer'])

        union_personal(titles,prices)

    except Exception as e:
        print(e)


def musimundo_samsung():
    try:        
        response = rq.get(scrapper.dataMusi()['urlMusi'])
        if response.status_code == 200:
            home = response.content.decode('utf-8')
            parsed = html.fromstring(home)
            prices = parsed.xpath(scrapper.dataMusi()['priceMusi'])
            titles = parsed.xpath(scrapper.dataMusi()['titleMusi'])
            
            container_titles = []
            container_prices = []

            for title in titles:
                rename = title.replace("\n\t\t\t\t\t\t\t","").replace("\n","").replace("  "," ").replace("CELULAR SAMSUNG ","")
                print(rename)
                container_titles.append(rename)

            for price in prices:
                new_price = str(price).replace("$","").replace(",00","").replace(".","")
                container_prices.append(float(new_price))

        union_musimundo(container_titles,container_prices)
        
    except Exception as e:
        print(e)

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""


"""
Function to create the different tables to populate the data warehouse
"""
def createTables():
    try:
        db.curExecute("""
            CREATE TABLE IF NOT EXISTS staging_musimundo_samsung(
                id int8 NOT NULL GENERATED BY DEFAULT AS IDENTITY, 
                name_cel VARCHAR(150),
                price float8,
                date timestamp
            );
            
            CREATE TABLE IF NOT EXISTS staging_personal_samsung(
                id int8 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
                name_cel VARCHAR(150),
                price float8,
                date timestamp
            );
            
            CREATE TABLE IF NOT EXISTS staging_movistar_samsung(
                id int8 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
                name_cel VARCHAR(150),
                price float8,
                date timestamp
            );

            CREATE TABLE IF NOT EXISTS int_samsung(
                id int8 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
                shop VARCHAR (80),
                name_cel VARCHAR(150),
                price float8,
                dolar_price float8,
                date timestamp,
                updated_by VARCHAR(50)

            );
            
            CREATE TABLE IF NOT EXISTS dw_samsung(
                id int8 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
                shop VARCHAR (80),
                celular VARCHAR(190),
                peso_price float8,
                dolar_price float8,
                updated timestamp,
                updated_by VARCHAR(50)
            );

        """)
        db.curExecute("SET DATESTYLE TO 'European';")
    
    except Exception as e:
        print(e)


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
    'scrapper_celulares_v1', 
    default_args = default_args, 
    schedule_interval = '*/15 12-23 * * 1-5',  
    start_date = days_ago(2), 
    tags = ['scrapper_phones_v1'], 

) as dag: 

    """
     We perform several inserts to ensure that we extract the data in its entirety
     The first thing we do is populate the different staging tables
    """
    insert_tables_staging_musimundo = PythonOperator(task_id = "Insert_into_table_staging_musimundo", python_callable = musimundo_samsung)
    insert_tables_staging_personal = PythonOperator(task_id = "Insert_into_table_staging_personal", python_callable = personal_samsung)
    insert_tables_staging_movistar = PythonOperator(task_id = "Insert_into_table_staging_movistar", python_callable = movistar_samsung)
    insert_tables_staging_musimundo_v2 = PythonOperator(task_id = "Insert_into_table_staging_musimundo_v2", python_callable = musimundo_samsung)
    insert_tables_staging_personal_v2 = PythonOperator(task_id = "Insert_into_table_staging_personal_v2", python_callable = personal_samsung)
    insert_tables_staging_movistar_v2 = PythonOperator(task_id = "Insert_into_table_staging_movistar_v2", python_callable = movistar_samsung)
    insert_tables_staging_musimundo_v3 = PythonOperator(task_id = "Insert_into_table_staging_musimundo_v3", python_callable = musimundo_samsung)
    insert_tables_staging_personal_v3 = PythonOperator(task_id = "Insert_into_table_staging_personal_v3", python_callable = personal_samsung)
    insert_tables_staging_movistar_v3 = PythonOperator(task_id = "Insert_into_table_staging_movistar_v3", python_callable = movistar_samsung)

    """
    We delete if duplicates are found
    """
    delete_repeat_musimundo = PythonOperator(task_id = "Delete_if_exists_repeat_table_musimundo", python_callable = deleteRepeatMusimundo)
    delete_repeat_personal = PythonOperator(task_id = "Delete_if_exists_repeat_table_personal", python_callable = deleteRepeatPersonal)
    delete_repeat_movistar = PythonOperator(task_id = "Delete_if_exists_repeat_table_movistar", python_callable = deleteRepeatMovistar)


    """
    Task flow 
    """
    insert_tables_staging_musimundo >> insert_tables_staging_personal >> insert_tables_staging_movistar >> insert_tables_staging_musimundo_v2 >> insert_tables_staging_personal_v2 >> insert_tables_staging_movistar_v2 >> insert_tables_staging_musimundo_v3 >> insert_tables_staging_personal_v3 >> insert_tables_staging_movistar_v3 >> [delete_repeat_musimundo,delete_repeat_personal,delete_repeat_movistar]
    


