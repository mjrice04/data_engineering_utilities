import os
import sys
import zipfile

directory = os.getcwd()
working_directory = os.path.split(os.getcwd())[1]
hdfs_prefix = '/data/fi/mortgage/raw/fi_mgr_raw/1010/'
uploaded_prefix = '/cloudera_nfs1/fi/mortgage/data/1010/'
print(directory)
print(working_directory)
for file in os.listdir(directory):
     if file.endswith(".zip"):
         z = zipfile.ZipFile(file, 'r')
         z.extractall(directory)
         z.close()
         txt_file = file[:-4] + '.txt'
         gzip_file = file[:-4] + '.txt.gz'
         os.system('sed 1d '+ txt_file + ' > x.txt')
         os.system('mv x.txt ' + txt_file)
         os.system('gzip ' + txt_file)
         os.system('hdfs dfs -put ' + gzip_file + hdfs_prefix + working_directory + '/')
         os.system('mkdir ' + directory + '/uploaded_data')
         os.system('mv ' + gzip_file + ' ' + uploaded_prefix + working_directory + '/uploaded_data/')
         os.system('rm ' + file)
         print(gzip_file + 'loaded') 
