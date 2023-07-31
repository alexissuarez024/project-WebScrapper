## Introducción
Este trabajo se centra en la extracción de datos en la web aplicando ***webscrapping***, usando herramientas como lxml y beatifulsoup para dar con la información.
Los datos que recolecté son sobre celulares en 3 distintas empresas que comercializan una marca particular y, sobre ella, tomando información sobre: 
- Precio de comercialización al dia que se realice la busqueda y su posterior actualización si es que la hay.
- Nombre y modelo del celular
- Empresa en la cual está siendo comercializado

A esta busqueda le agregue una recolección más, utilizando una página que se centra en visualizar el precio de la cotización del dolar en Argentina, tomando asi distintos tipos de cambio.

Una vez  extraidos y transformado los datos son cargados a un modelo de ***tablas staging*** para poder trabajar sobre ellos en la base de datos. Una vez allí se verifica que no haya duplicidad de data y que tengan coherencia en su alojamiento. Posteriormente se crea una tabla ***interfaz***  en la que se unificaran los datos para poder aplicar  un cálculo sobre el precio, comparando la información que nos da la tabla de dolares, y asi poder obtener una nueva dimensión que sera el precio en dolares. 

Cuando se tengan los datos unificados y actualizados, podemos ingestarlos en el ***data warehouse*** y a la par generamos un archivo ***csv***  sobre la corrida del dia con la información de la base de datos. Teniendo asi distintos modelos de una marca que es comercializada por 3 empresas y calculando los precios al día de la fecha.


## El proyecto
Este proyecto esta configurado de forma local y puede ser seguido de dos formas: 
- Sobre  ***Airflow***  en donde su ambiente es levantado mediante un contenedor de ***docker compose***.
- O de igual forma si desea puede correrlo mediante CLI.

Ambas alternativas estan alojadas en carpetas *dags* (airflow) y *terminal* (CLI)
En ellas encontrara el mismo proyecto pero seguido de distinta forma

> Es recomendable crear un ambiente virtual para poder tener las dependencias alojadas alli y que no le generen conflicto, en los archivos encontrará un archivo requeriments.txt con las dependencias necesarias

Por ultimo asegurese de colocar los datos correspondientes a su conexión de base de datos, en este caso se utiliza una conexión local de Postgres, en el archivo *config_connection*, ubique:
```python
            config = { "host" : os.environ.get("HOST"),
                "database" : os.environ.get("DATABASE"),
                "user" : os.environ.get("USER"),
                "password" : os.environ.get("PASSWORD")}

```
Y reemplace los valores con los suyos para poder generar la conexión con exito.

Sientase libre de comentar, usar o modificar estos archivos, son trabajos que realizo con el fin de presentarme y conectarme con los demás.
Un saludo :)


>Aclaración: El orden para ejecutar los archivos es:
- scrapper_dolar
- scrapper_celulares
- main