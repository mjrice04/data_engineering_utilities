def zip_file_download(url,directory):
    """
    Purpose: To download a zipfile from a webisite and extract it at a chosen
    directory

    Parameters: 
    url - is the url where the file is located at
    directory - the directory to unzip to

    No output
    """
    r = requests.get(url, cookies=cookie, stream=True) #working with reques api to get zipfile, pass in the cookies
    z = zipfile.ZipFile(io.BytesIO(r.content)) #using zipfile package to grab the contents of my http request and bring it back as a zipfile
    try:
        z.extractall(directory) #bringing the file to my determined directory
        z.close() #close the open file and unzip it!
        return('Download Successful')
    except:
        return('Download Unsuccessful')
