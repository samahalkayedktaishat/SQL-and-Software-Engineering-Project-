#!/usr/bin/env python
# coding: utf-8

# In[2]:


#importing the required libraries
import argparse as agp
import getpass
import os
import numpy as np 
#importing from the module 
from myBox import MSSQL_DBConnector as mssql
from myBox import DBConnector as dbc
import myBox.ContentObfuscation as ce
from pandas.util.testing import assert_frame_equal

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

 
    
#definition of the command line arguments function 
def processCLIArguments()-> dict:
    
    retParametersDictionary:dict = None
    
    dbpassword:str = ''
    obfuscator: ce.ContentObfuscation = ce.ContentObfuscation()

    try:
        argParser:agp.ArgumentParser = agp.ArgumentParser(add_help=True)

        argParser.add_argument("-n", "--DSN", dest="dsn",                                 action='store', default= None, help="Sets the SQL Server DSN descriptor file - Take precedence over all access parameters", type=str)
        argParser.add_argument('-s',"--dbserver",                                 action='store', help="Sets the dbserver for the SQL Server connection ", type=str)
        argParser.add_argument('-m',"--dbname",                                 action='store', help="Sets the dbname for the SQL Server connection ", type=str)
        argParser.add_argument('-u',"--dbusername",                                 action='store', help="Sets the dbusername for the SQL Server connection ", type=str)
        argParser.add_argument('-p',"--dbuserpassword",                                 action='store', help="Sets the dbuserpassword for the SQL Server connection ", type=str)
        argParser.add_argument("-t","--trustedmode",                                 action='store', default= 'yes', help="Sets the trustedmode for the SQL Server connection ", type=bool)
        argParser.add_argument("-v","--viewname",                                 action='store', default= None, help="Enables to view the name for the SQL Server connection ", type=str)
        argParser.add_argument("-f","--persistencefilepath",                                 action='store',help="Enables to see if the persistence file path for survey structure table exsists ", type=str)
        argParser.add_argument("-r","--resultsfilepath",                                 action='store',help="Enables to view the result of the persistence file path for survey structure", type=str)
        
        
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


# In[11]:


#function to Read [dbo].[SurveyStructure] table to pandas dataframe
def getSurveyStructure(connector: mssql.MSSQL_DBConnector) -> pd.DataFrame:
    
    surveyStructResults =connector.ExecuteQuery_withRS('SELECT * FROM [dbo].[SurveyStructure]')
    
    return surveyStructResults


# In[12]:


#call to the getSurveyStructure function 
surveyStructResults = getSurveyStructure(mssql.MSSQL_DBConnector)


# In[7]:


# function to check if a Persistence File Exist to save the latest version of SurveyStructure table 
def doesPersistenceFileExist(persistenceFilePath: str)-> bool:
  
    success = os.path.exists(persistenceFilePath)

    return success


# In[8]:


# function to check if the Persistence File to save the latest version of SurveyStructure table is writable 
def isPersistenceFileDirectoryWritable(persistenceFilePath: str)-> bool:
    
    f = open(persistenceFilePath, "a")
    success =f.writable()
    
    return success


# In[9]:


# function to compare surveyStructResults to the persistenceFilePath
def compareDBSurveyStructureToPersistenceFile(surveyStructResults:pd.DataFrame, persistenceFilePath: str) -> bool:
   
    try:
        assert_frame_equal(surveyStructResultsDF,persistenceFilePathDF, check_dtype=False)
        print("The dataframes are the same.")
    except: 
        print("Please verify data integrity.")
        
    return None   
  


# In[9]:


#function that Replicates the algorithm of the dbo.fn_GetAllSurveyDataSQL stored function
def getAllSurveyDataQuery(connector: dbc.DBConnector) -> str:

     #IN THIS FUNCTION YOU MUST STRICTLY CONVERT THE CODE OF getAllSurveyData written in T-SQL, available in Survey_Sample_A19 and seen in class
    # The Python version returns the string containing the dynamic query (as we cannot use sp_executesql in Python!)
    
    strQueryTemplateForAnswerColumn: str = """COALESCE( 
				( 
					SELECT a.Answer_Value 
					FROM Answer as a 
					WHERE 
						a.UserId = u.UserId 
						AND a.SurveyId = <SURVEY_ID> 
						AND a.QuestionId = <QUESTION_ID> 
				), -1) AS ANS_Q<QUESTION_ID> """ 


    strQueryTemplateForNullColumnn: str = ' NULL AS ANS_Q<QUESTION_ID> '

    strQueryTemplateOuterUnionQuery: str = """ 
			SELECT 
					UserId 
					, <SURVEY_ID> as SurveyId 
					, <DYNAMIC_QUESTION_ANSWERS> 
			FROM 
				[User] as u 
			WHERE EXISTS 
			( \
					SELECT * 
					FROM Answer as a 
					WHERE u.UserId = a.UserId 
					AND a.SurveyId = <SURVEY_ID> 
			) 
	"""

    strCurrentUnionQueryBlock: str = ''

    strFinalQuery: str = ''
    strFinalQuery=pd.DataFrame()    
    #Cursors are replaced by a query retrived in a pandas df to be used in the loops    
    surveyQuery:str = 'SELECT SurveyId FROM Survey ORDER BY SurveyId' 
    surveyQueryDF = connector.ExecuteQuery_withRS(surveyQuery)   
    
    #Cursors are replaced by a query retrived in a pandas df to be used in the loops
    questionCursor:str='SELECT QuestionId FROM Question'
    questionQueryDF = connector.ExecuteQuery_withRS(questionCursor)   
      

        
    #this Main loop over each survey Checks if current QuestionId is in SurveyStructure table: Yes flag InSurvey = 1 ,No flag InSurvey = 0 '''

    for surveyId in surveyQueryDF.SurveyId :
        
        currentQuestionCursor='''SELECT *
					FROM
					(
						SELECT
							SurveyId,
							QuestionId,
							1 as InSurvey
						FROM
							SurveyStructure
						WHERE
							SurveyId ='''+ str(surveyId) +\
						'''UNION
						SELECT ''' + str(surveyId) + \
							''' as SurveyId , Q.QuestionId,
							0 as InSurvey
						FROM
							Question as Q
						WHERE NOT EXISTS
						(
							SELECT *
							FROM SurveyStructure as S
							WHERE S.SurveyId = ''' + str(surveyId) + '''AND S.QuestionId = Q.QuestionId
						)
					) as t
					ORDER BY QuestionId;
	'''
        
        #Cursors are replaced by a query retrived in a pandas df to be used in the loops 
        QuestionIN_DF= connector.ExecuteQuery_withRS(currentQuestionCursor)

        dfStrColumnQueryPart = pd.DataFrame()
        dfStrColumnQueryPart = dfStrColumnQueryPart.fillna(0)    

    
        #inner loop, over the questions of each survey in currentSurveyId to construct the ANSWER COLUMN queries '''

        for questionId in questionQueryDF.QuestionId :
            
            for i in QuestionIN_DF['InSurvey']:
                if i==1:
                    
                    query_getAnswer = strQueryTemplateForAnswerColumn.replace('<QUESTION_ID>',str(questionId),2)
                    
                else:
                    
                    query_getNoAnswer = strQueryTemplateForNullColumnn.replace('<QUESTION_ID>',str(questionId))
                    
                    
                    
                    
            CurrentUnionQueryBlock=strQueryTemplateOuterUnionQuery.replace('<DYNAMIC_QUESTION_ANSWERS>',str(query_getAnswer))
            CurrentUnionQueryBlock=CurrentUnionQueryBlock.replace('<SURVEY_ID>',str(surveyId))
        
            strFinalQuery1=connector.ExecuteQuery_withRS(CurrentUnionQueryBlock)
            
            strFinalQuery=pd.concat([strFinalQuery,strFinalQuery1])
            
            
            print(surveyId ,questionId)

        
        
    return strFinalQuery

# call to the function getAllSurveyDataQuery
getAllSurveyDataQuery(dbc.DBConnector)


# In[2]:


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
            #Make a connection to database using the command line Arguments inserted in the command prompt 
            connector = mssql.MSSQL_DBConnector(DSN = cliArguments["dsn"], dbserver = cliArguments["dbserver"],                 dbname = cliArguments["dbname"], dbusername = cliArguments["dbusername"],                 dbpassword = cliArguments["dbuserpassword"], trustedmode = cliArguments["trustedmode"],                 viewname = cliArguments["viewname"])

            #check if the connection is established 
            connector.Open()
            
            surveyStructureDF = getSurveyStructure(connector)

            if(doesPersistenceFileExist(cliArguments["persistencefilepath"]) == False):

                if(isPersistenceFileDirectoryWritable(cliArguments["persistencefilepath"]) == True):
                   
                    #pickle the dataframe in the path given by persistencefilepath
                    surveyStructResults.to_csv(cliArguments["persistencefilepath"])
                    print("\nINFO - Content of SurveyResults table pickled in " + cliArguments["persistencefilepath"] + "\n")
               
            else:
                #Compare the existing pickled SurveyStructure file with surveyStructureDF
                try:
                    assert_frame_equal(surveyStructResults,cliArguments["persistencefilepath"], check_dtype=False)
                    print("The dataframes are the same.")
                except: 
                    print("Please verify data integrity.")
                    surveyStructResults.to_csv(cliArguments["persistencefilepath"])

            #get your survey results from the view in a dataframe and save it to a CSV file in the path given by resultsfilepath
            surveyStructResults=getSurveyStructure(mssql.MSSQL_DBConnector)
            surveyStructResults.to_csv(cliArguments["resultsfilepath"])
            
            print("\nDONE - Results exported in " + cliArguments["resultsfilepath"] + "\n")

            connector.Close()

        except Exception as excp:
            print(excp)
    else:
        print("Inconsistency: CLI argument dictionary is None. Exiting")
        return



if __name__ == '__main__':
    main()


# In[ ]:


python Python_SQL_Project_DS_SAMAH.py -n A20 -s DESKTOP-Q3K8T8H\SQL2019 -m Survey_Sample_A19  -u samah  -p gAAAAABgXhdyUW-y-sC_5WBpuRXF2Yr5Ppkmq4LDsPC5LvorxH1yTRnGoHVB2lm3piwqaRmgBhRs-5BuR2TsYJ507IEvBjo0fg==


# In[4]:


b=ce.ContentObfuscation()
passoff=b.obfuscate('Samaher@92')
print(passoff)


# In[ ]:




