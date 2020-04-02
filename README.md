# Cargill
código do teste
# Foi inibido o log completo para pegar somente os "ERROR"
# Ao executar o shell, voce deve passar dois paramatros, o primeiro é o "input_path" (caminho onde o arquivo está localizado) e o segundo é "output_path" (caminho onde o arquivo será gravado). Ex: ./teste.sh input output

## Iniciar projeto
* Innstalar biblioteca pyspark atraves de requirements.txt
    * pip install -r requirements.txt
* Apos instalação de pacotes, seguir seguinte comando:
    ```sh run_spark.sh ```
    * Comando inicia spark job com spark-submit setado

## Respostas
*  4.1 Which is the older project fully supported
   * Help RIZ Make A Charity Album: 8 Songs, 8 Causes, 1 Song For Each Cause (Canceled)|1970-01-01 01:00:00|
* 4.2 Which is the main category with the most pledged ammount
   * Games|18975879.60|
* 4.3 What is the average pledged ammount in USD for each category 
   * Food|6497.381452|
   * Games|8520.507846|
   * Music|4652.214944|
* 6 Tell us how would you save these entries on an Impala database
```
* I would save the SparkDataFrame content to the Impala database table via JDBC
first, I need the URL in the Impala database.
For example:
url = "jdbc: impala: imp_env; auth = noSas"
Second, my steps for writing data using JDBC connections from the Impala database in PySpark:
(I can use the URL or the variable I created for the URL in this step)
df_joiner.write.mode ("attach") .jdbc (url = "jdbc: impala: //10.61.1.101: 21050 / test; auth = noSas", table = "ks_projects_201612", pro)
mode: one of the 'attach', 'replace', 'error' and 'ignore' modes.
URL: URL of the JDBC database in the jdbc: subprotocol: subname format.
tableName: the name of the table in the Impala database.
```