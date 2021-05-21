#!/usr/bin/env python
# coding: utf-8

# In[9]:


#!/usr/bin/env python
# coding: utf-8

# In[7]:


import platform

from myBox import DBConnector as db
import myBox.ModuleInstaller as mi

try:
    import pyodbc
except:
    mi.installModule("pyodbc")
    import pyodbc

    
    
class MSSQL_DBConnector(db.DBConnector):
    """This class inherits from the abstract class _DBConnector and implements _selectBestDBDriverAvailable for a MSSQL server connection"""

    def __init__(self: object, DSN, dbserver: str, dbname: str, dbusername: str, dbpassword: str, trustedmode: bool =  False, viewname: str = "", isPasswordObfuscated:bool = True):
        
        super().__init__(DSN = DSN, dbserver = dbserver, dbname= dbname, dbusername = dbusername,dbpassword = dbpassword, trustedmode = trustedmode, viewname = viewname, isPasswordObfuscated = isPasswordObfuscated)

        self._selectBestDBDriverAvailable()

    def _selectBestDBDriverAvailable(self: object) -> None:
        
        self.selectedDriver='ODBC Driver 17 for SQL Server'
        
    
   
            


# In[ ]:




