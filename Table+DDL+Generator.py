
# coding: utf-8

# In[16]:


"""
Table DDL Generator
Purpose: Creates Hadoop Table DDLs templates automatically
Written by: Matt Rice
Created 1/8/2018
"""


# In[17]:


def create_text(path, file_name, database, table_name, write_columns, columns, col_types):
    with open(path + file_name, 'w') as f:
        f.write('use database;\n')
        f.write('\n')
        f.write('create external table ' + database + '.' + table_name + '(\n' )
        if write_columns == True:
            for counter, value in enumerate(columns):
                if not counter == len(columns):
                    f.write(value + ' ' + col_types[counter] + ',\n')
                else:
                    f.write(value + ' ' + col_types[counter] + '\n')
        f.write(')\n')
        status = False
        while status == False:                            
            type_hadoop = input('Is the raw data a csv (Please type y or n)')
            if type_hadoop == 'y' or type_hadoop == 'n':
                status = True
        end_of_raw = type_of_hadoop(type_hadoop)
        f.write(end_of_raw)
                
                                      
                                      


# In[18]:


def type_of_hadoop(type_hadoop):
    delim = input('Enter delimiter: ')
    newline = input('Enter newline character: ')
    location = input('Enter hdfs location: ')
    skip_first_line = input('Skip the first line in raw file (Enter y or n)?')
    if skip_first_line == 'y':
        tbl_props = '\ntblproperties("skip.header.line.count"="1");'
    else: 
        tbl_props = ';'
    if type_hadoop == 'y':
        end_of_raw = """ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES ( 
            'separatorChar' = '%s',
            'quoteChar'     = '"',
            'escapeChar'    = '\\'
                            )
STORED AS TEXTFILE
LOCATION "%s"
%s""" % (delim, location, tbl_props)
    else:
        end_of_raw = """ROW FORMAT DELIMITED
FIELDS TERMINATED BY '%s'
LINES TERMINATED BY '%s'
STORED AS TEXTFILE
LOCATION "%s"%s""" % (delim,newline,location, tbl_props)
    return end_of_raw

        


# In[23]:


path = ''
file_name ='test.ddl'
database = 'fi_mgr'
table_name = 'test_script'
write_columns = True
columns = ['a','b','c','d','e','f']
col_types = ['string', 'int', 'string', 'decimal(4,2)', 'string', 'int']


# In[24]:


create_text(path, file_name, database, table_name, write_columns, columns, col_types)


# In[ ]:




