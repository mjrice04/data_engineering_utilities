
import os
import subprocess

def hdfs_ls(hdfs_directory):
    """Runs a hdfs command and returns output of the command.

    Parameters
    ----------

    hdfs_directory - hdfs directory passed into function

    Returns
    ---------
    
    returns the output of the command
  
    """
    result = subprocess.run(['hdfs', 'dfs', '-ls', hdfs_directory],
    stdout=subprocess.PIPE)
    output = result.stdout.decode('utf-8')
    return output



def hdfs_put(hdfs_directory,file_name):
    """Runs a hdfs put command to place a file into hdfs

    Parameters
    ----------

    hdfs_directory - hdfs directory passed into function

    file_name - the name of the file to upload to hdfs

    Returns
    ---------
    
    returns the output of the command
  
    """
    result = subprocess.run(['hdfs', 'dfs', '-put', file_name, hdfs_directory],
    stdout=subprocess.PIPE)
    output = result.stdout.decode('utf-8')
    return output



def hdfs_get(hdfs_directory,file_name):
    """Runs a hdfs get command to pull a file from hdfs

    Parameters
    ----------

    hdfs_directory - hdfs directory passed into function

    file_name - the name of the file to upload to hdfs

    Returns
    ---------
    
    returns the output of the command (if there is output)
  
    """
    result = subprocess.run(['hdfs', 'dfs', '-get', hdfs_directory + file_name],
    stdout=subprocess.PIPE)
    output = result.stdout.decode('utf-8')
    return output


