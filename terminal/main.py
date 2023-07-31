"""""""""
Import of script utilities
"""""""""
import psycopg2
from config_connection import Connection
from zona_horaria import date_now

"""""""""
Instance of global variable
"""""""""
db = Connection(psycopg2)


"""""""""
Function that generates several insertions to ensure that all the data is inserted and once the run is complete, 
it deletes the data that is found to be duplicated to leave the data warehouse ready
"""""""""
def chargeDW():
    # print("Load DW?")
    # print("YES or NO = [y/n]")
    # response = str(input()).lower()

    # if response == 'y':
        d = 0
        while d < 3:
            insert_dw()
            d += 1

        repeat_dw()

    # elif response == 'n':
    #     print('Without changes')

    # else:
    #     print('Incorrect input')


"""""""""
5) Function to find and eliminate duplicates in the last run of the data warehouse
"""""""""
def repeat_dw():
    try:

        responseDW = db.curFetchAll("""
            with c as (
                select * from 
                                ( select *, COUNT(*) over (partition by celular, peso_price, dolar_price order by id) as duplicado
                                from dw_samsung
                                ) as A
                where duplicado > 1 
            ) select id,case 
                when duplicado > 1 then 'repeat' 
            end flag
            from c
        
            """)
        
        print(responseDW)
        print(type(responseDW))

        for resDW in responseDW:
                r = 0
                print(resDW)
                print('There are repeated')
                if str(resDW[1]) == 'repeat':
                    print('ITERABLE',resDW[r])
                    print('Equal')
                    db.curExecute(f"""
                        DELETE FROM dw_samsung WHERE id = {resDW[r]}
                    """)
                    r = r + 1

                else: 
                    print('Dont equal')

    except Exception as e:
        print(e)        


"""""""""
4) Here we consult the interface table in search of finding data prepared to be inserted into the data warehouse
"""""""""
def insert_dw():
    try:

        responseInt = db.curFetchAll("""
            select shop,name_cel, price, dolar_price, updated_by from int_samsung;
        """)
        print(responseInt)
        for res in responseInt:
            resShop = res[0]
            resCel = res[1]
            resPeso = int(res[2])
            resDolar = int(res[3])
            resBy = res[4]
        
            db.curExecute(f"""
                insert into dw_samsung (shop,celular,peso_price,dolar_price,updated,updated_by) values
                ('{resShop}','{resCel}',{resPeso},{resDolar},'{date_now}','{resBy}')
            """)

        print('Charge of the data warehouse completed successfuly')

    except Exception as e:
        print('Error ', e)


"""""""""
3) Function to find and delete duplicates in the interface table
"""""""""
def delete_repeat():
    try:
        responseInt = db.curFetchAll("""
            with c as (
                select * from 
                                ( select *, COUNT(*) over (partition by name_cel,price order by id) as duplicado
                                from int_samsung
                                ) as A
                where duplicado > 1 
            ) select id,case 
                when duplicado > 1 then 'repeat' 
            end flag
            from c
        
            """)
        
        print(responseInt)
        print(type(responseInt))

        for resInt in responseInt:
                r = 0
                print(resInt)
                print('There are repeated')
                if str(resInt[1]) == 'repeat':
                    print('ITERABLE',resInt[r])
                    print('Equal')
                    db.curExecute(f"""
                        DELETE FROM int_samsung WHERE id = {resInt[r]}
                    """)
                    r = r + 1

                else: 
                    print('Dont equal')

    except Exception as e:
        print(e)    


"""""""""
2) In this function we consult the different staging tables and, with the data previously obtained from the dollar, 
   we calculate the prices in dollars and Argentine peso
"""""""""
def insert_int(dolarBlue):
    
    try:    
        moviShop = 'Movistar'
        perShop = 'Personal'
        musiShop = 'Musimundo'

        print('Who will do the insertion?')
        user = str(input()).lower()

        """
        Consultation and calculation section of the Movistar staging table 
        and subsequent insertion
        """
        responseMovi = db.curFetchAll(f"""
            select name_cel , price ,sum(price / {dolarBlue}) precio_dolar  from staging_movistar_samsung
            group by name_cel , price; 
        """)
        

        print(responseMovi)
        for x in responseMovi:
            cel_type_movi = x[0]
            cel_price_movi = int(x[1])
            cel_in_dolar_price_movi = int(x[2])
            db.curExecute(f"""
                insert into int_samsung (shop,name_cel,price,dolar_price,date,updated_by)
                values('{moviShop}','{cel_type_movi}',{cel_price_movi},{cel_in_dolar_price_movi},'{date_now}','{user}');
                """)
        

        """
        Consultation and calculation section of the Musimundo staging table 
        and subsequent insertion
        """

        responseMusi = db.curFetchAll(f"""
            select name_cel , price ,sum(price / {dolarBlue}) precio_dolar  from staging_musimundo_samsung
            group by name_cel , price; 
        """)
            
        print(responseMusi)
        for x in responseMusi:
            cel_type_musi = x[0]
            cel_price_musi = int(x[1])
            cel_in_dolar_price_musi = int(x[2])
            db.curExecute(f"""
                insert into int_samsung (shop,name_cel,price,dolar_price,date,updated_by)
                values('{musiShop}','{cel_type_musi}',{cel_price_musi},{cel_in_dolar_price_musi},'{date_now}','{user}');
                """)

        """
        Consultation and calculation section of the Personal staging table 
        and subsequent insertion
        """             
        responsePer = db.curFetchAll(f"""
            select name_cel , price ,sum(price / {dolarBlue}) precio_dolar  from staging_personal_samsung
            group by name_cel , price; 
        """)
            
        print(responsePer)
        for x in responsePer:
            cel_type_per = x[0]
            cel_price_per = int(x[1])
            cel_in_dolar_price_per = int(x[2])
            db.curExecute(f"""
                insert into int_samsung (shop,name_cel,price,dolar_price,date,updated_by)
                values('{perShop}','{cel_type_per}',{cel_price_per},{cel_in_dolar_price_per},'{date_now}','{user}');
                """)       
    
    except Exception as e:
        print(e)


"""""""""
1) We consult the dollar data previously loaded in the database
"""""""""
def dolarBlue():
    try:
        
        responseDolar = db.curFetchOne("select sell_values from scrapper_dolar_v1 where dolar_types like 'DÓLAR BLUE' order by date_actualization desc")
        dolarBlue = responseDolar[0]
        print(dolarBlue)
        insert_int(dolarBlue)

    except Exception as e:
        print(e)


if __name__ == '__main__':

    dolarBlue()
    delete_repeat()
    print('\n')
    print("Load Data Warehouse?")
    print("YES or NO => [y/n]")
    response = str(input()).lower()

    if response == 'y':
        d = 0
        while d < 3:
            insert_dw()
            d += 1

        repeat_dw()

    elif response == 'n':
        print('Without changes')

    else:
        print('Incorrect input')

