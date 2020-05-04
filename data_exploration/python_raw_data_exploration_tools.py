import pandas
import os
import re

def read_n_Lines(FilePath, N):
    with open(FilePath, 'r') as f:
        for i,line in enumerate(f):
            print(i, line)
            if i == N + 1:
                f.close()
                break;

def find_n_lines(FilePath, N):
    with open(FilePath, 'r') as f:
        for i,line in enumerate(f):
            if N - 5 < i < N + 5:
                print(i, line)
        f.close()


def data_length_guess(FilePath, N):
    with open(FilePath, 'r') as f:
        LengthList = []
        HighestLength = []
        for i, line in enumerate(f):
            FieldList = list(line.split('|'))
            length = [len(x) for x in FieldList]
            print(length)
            if i == 1: #do not need length of first line
                HighestLength.append(length)
                if i > 1: #need two rows to compare
                    LengthList.append(length)
                    for i in range(len(HighestLength)):
                        if LengthList[i] > HighestLength[i]: #checking the two arrays
                            ReplaceValue = LengthList
                            HighestLength[i] = ReplaceValue
            if i == N:
                print(HighestLength[0])
                f.close()
                break;

def ddl_generator(FilePath, LogPath, TableName, Partition = False):
    DDLString = """
                BULK INSERT src.%s
                FROM '%s' 
                WITH ( 
                  FIRSTROW = 2
                , BATCHSIZE = 100000
                , FIELDTERMINATOR  = '|'
                , ROWTERMINATOR = '\\n'
                , ERRORFILE = '%s\LoadLogs\%s.log'
                , MAXERRORS = 1 
                )
                ;
                GO
                """ % (TableName, FilePath, LogPath, TableName)
    with open(FilePath, 'r') as f:
        for i, line in enumerate(f):
            Cols = line.split('|')
            while i < 1:
                for x in range(len(Cols)):
                    if x == 0: #First Column in DDL
                        print('CREATE TABLE src.%s (\n  %s NVARCHAR( )' % (TableName,Cols[x]))
                    else:
                        print(', ' + Cols[x] + ' NVARCHAR( )')
                    if x == (len(Cols)-1): #Last Column in DDL
                        if Partition == True:
                            print(')\nON psINSERTPartitionSchemeHere(FIELD)\nWITH (DATA_COMPRESSION = PAGE);')
                        else:
                            print(')\nWITH (DATA_COMPRESSION = PAGE);')
                        print(DDLString)
                i += 1
            break;
        f.close()


def files_to_load(Path, FileExtension):
    Files = os.listdir(Path)
    DataFiles = [x for x in Files if FileExtension in x.lower()]
    return DataFiles


    










