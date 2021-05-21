#!/usr/bin/env python
# coding: utf-8

# In[13]:


import argparse as agp
import getpass
import os

from myBox import MSSQL_DBConnector as mssql
from myBox import DBConnector as dbc
import myBox.ContentObfuscation as ce


try:
    import pandas as pd
except:
    mi.installModule("pandas")
    import pandas as pd



def printSplashScreen():
    print("*************************************************************************************************")
    print("\t THIS SCRIPT ALLOWS TO EXTRACT SURVEY DATA FROM THE SAMPLE SEEN IN SQL CLASS")
    print("\t IT REPLICATES THE BEHAVIOUR OF A STORED PROCEDURE & TRIGGER IN A PROGRAMMATIC WAY")
    print("\t COMMAND LINE OPTIONS ARE:")
    print("\t\t -h or --help: print the help content on the console")
    print("*************************************************************************************************\n\n")



def processCLIArguments()-> dict:
    
    retParametersDictionary:dict = None
    
    dbpassword:str = ''
    obfuscator: ce.ContentObfuscation = ce.ContentObfuscation()

    try:
        argParser:agp.ArgumentParser = agp.ArgumentParser(add_help=True)

        argParser.add_argument("-n", "--DSN", dest="dsn",                                 action='store', default= None, help="Sets the SQL Server DSN descriptor file - Take precedence over all access parameters", type=str)
        argParser.add_argument(dest="dbserver",                                 action='store', default= None, help="Sets the dbserver for the SQL Server connection - Take precedence over all access parameters", type=str)
        argParser.add_argument(dest="dbname",                                 action='store', default= None, help="Sets the dbname for the SQL Server connection - Take precedence over all access parameters", type=str)
        argParser.add_argument(dest="dbusername",                                 action='store', default= None, help="Sets the dbusername for the SQL Server connection - Take precedence over all access parameters", type=str)
        argParser.add_argument(dest="dbuserpassword",                                 action='store', default= None, help="Sets the dbuserpassword for the SQL Server connection - Take precedence over all access parameters", type=str)
        argParser.add_argument(dest="trustedmode",                                 action='store', default= None, help="Sets the trustedmode for the SQL Server connection - Take precedence over all access parameters", type=bool)
        argParser.add_argument(dest="viewname",                                 action='store', default= None, help="Enables to view the name for the SQL Server connection - Take precedence over all access parameters", type=str)
        argParser.add_argument(dest="persistencefilepath",                                 action='store', default= None, help="Enables to see if the persistence file path for survey structure table exsists - Take precedence over all access parameters", type=bool)
        argParser.add_argument(dest="resultsfilepath",                                 action='store', default= None, help="Enables to view the result of the persistence file path for survey structure  - Take precedence over all access parameters", type=bool)
        
        
        argParsingResults=argParser.parse_args()
        
        

        retParametersDictionary = {
                    "dsn" : argParsingResults.dsn,        
                    "dbserver" : argParsingResults.dbserver,
                    "dbname" : argParsingResults.dbname,
                    "dbusername" : argParsingResults.dbusername,
                    "dbuserpassword" : dbpassword,
                    "trustedmode" : argParsingResults.trustedmode,
                    "viewname" : argParsingResults.viewname,
                    "persistencefilepath": argParsingResults.persistencefilepath,
                    "resultsfilepath" : argParsingResults.resultsfilepath
                }

    except Exception as e:
        print("Command Line arguments processing error: " + str(e))

    return retParametersDictionary


# In[7]:


b=ce.ContentObfuscation()
passoff=b.obfuscate('Samaher@92')
print(passoff)


# In[9]:


mssql_connector=mssql.MSSQL_DBConnector('A20','DESKTOP-Q3K8T8H\SQL2019','Survey_Sample_A19','samah',passoff,'Trusted_Connection=yes')


# In[11]:


mssql_connector.Open()


# In[14]:


def main():
    
    cliArguments:dict = None

    printSplashScreen()

    try:
        cliArguments = processCLIArguments()
    except Except as excp:
        print("Exiting")
        return

    if(cliArguments is not None):
        
        #if you are using the Visual Studio Solution, you can set the command line parameters within VS (it's done in this example)
        #For setting your own values in VS, please make sure to open the VS Project Properties (Menu "Project, bottom choice), tab "Debug", textbox "Script arguments"
        #If you are trying this script outside VS, you must provide command line parameters yourself, i.e. on Windows
        #python.exe Python_SQL_Project_Sample_Solution --DBServer <YOUR_MSSQL> -d <DBName> -t True
        #See the processCLIArguments() function for accepted parameters

        try:
            connector = mssql.MSSQL_DBConnector(DSN = cliArguments["A20"], dbserver = cliArguments["DESKTOP-Q3K8T8H\SQL2019"],                 dbname = cliArguments["Survey_Sample_A19"], dbusername = cliArguments["samah"],                 dbpassword = cliArguments["gAAAAABgVj3QjUcZWtaeAYNnHDbQMGQZAMgelvmLFUnf9RhSPHgUmsqaAFL161zam6MGP7JTRsm9HADDb_fgLDH-fQASt4UiGA=="], trustedmode = cliArguments["yes"],                 viewname = cliArguments["viewname"])


            connector.Open()
            surveyStructureDF:pd.DataFrame = getSurveyStructure(connector)

            if(doesPersistenceFileExist(cliArguments["persistencefilepath"]) == False):

                if(isPersistenceFileDirectoryWritable(cliArguments["persistencefilepath"]) == True):
                    
                    
                    #pickle the dataframe in the path given by persistencefilepath
                    #TODO

                    print("\nINFO - Content of SurveyResults table pickled in " + cliArguments["persistencefilepath"] + "\n")
                    
                    #refresh the view using the function written for this purpose
                    #TODO
                    
            else:
                #Compare the existing pickled SurveyStructure file with surveyStructureDF
                # What do you need to do if the dataframe and the pickled file are different?
                #TODO
                pass #pass only written here for not creating a syntax error, to be removed
            
            #get your survey results from the view in a dataframe and save it to a CSV file in the path given by resultsfilepath
            #TODO

            print("\nDONE - Results exported in " + cliArguments["resultsfilepath"] + "\n")

            connector.Close()

        except Exception as excp:
            print(excp)
    else:
        print("Inconsistency: CLI argument dictionary is None. Exiting")
        return



if __name__ == '__main__':
    main()


# In[17]:





# In[16]:


cliArguments.


# In[ ]:




