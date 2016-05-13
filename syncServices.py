import httplib
import urllib
import urlparse
import json
import os
import logging
import time
import hashlib


'''
    This script will create, update, or delete service on destination server (dst) based on changes to the source server (src).

    You must setup rsync to replicate changes from src to dst before running this script.  


    You'll need to set up keys to allow ags1 to copy files without password.
    ssh-keygen
    ssh-copy-id

    Setting up rsync on chron
    Create a file /home/ags1/rsync_exclude.txt
    SampleWorldCities.MapServer/*
    System/*
    Utilities/*
    :wq

    Cron this command
    */1 * * * * rsync -azvv ags1@c67a:/home/ags1/arcgis/server/usr/directories/arcgissystem/arcgisinput /home/ags1/arcgis/server/usr/directories/arcgissystem/ --exclude-files /home/ags1/rsync_exclude.txt

    You could redirect the command outputs to a file to see the results every minute and also gives you an idea of the last time sync was run
'''


# Admin User for AGS Site
src_user = "siteadmin"
src_pw = "siteadmin"
src_serverurl = "http://c67ags1.jennings.home:6080"
src_home = "/home/ags"

dst_user = "siteadmin"
dst_pw = "siteadmin"
dst_serverurl = "http://c67ags2.jennings.home:6080"
dst_home = "/home/ags"

# Logging
log_to_file = False
log_level = logging.DEBUG
log_file_basename = "syncServices"
log = logging.getLogger()

# Create standard headers for all json requests
headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}  

def getToken(username, password, server, ssl=False):
    # Token URL is typically http://server[:port]/arcgis/admin/generateToken
    tokenURL = "/arcgis/admin/generateToken"
    
    # URL-encode the token parameters:-
    params = urllib.urlencode({'username': username, 'password': password, 'client': 'requestip', 'f': 'json'})
    
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
    
    # Connect to URL and post parameters
    httpConn = None
    if ssl:
        httpConn = httplib.HTTPSConnection(server)
    else:
        httpConn = httplib.HTTPConnection(server)
    httpConn.request("POST", tokenURL, params, headers)
    
    # Read response
    response = httpConn.getresponse()
    if (response.status != 200):
        httpConn.close()
        log.exception("Error while fetch tokens from admin URL. Please check the URL and try again.")
        return
    else:
        data = response.read()
        httpConn.close()
           
        # Extract the token from it
        token = json.loads(data)        
        return token['token']


def copy_data_stores(src_Token,src_httpConn,dst_Token,dst_httpConn):
    try:
        logging.debug("Begin Copy Data Stores")
        # Path to find data items
        findItems = "/arcgis/admin/data/findItems"
        registerItem = "/arcgis/admin/data/registerItem"

        params = urllib.urlencode({'token': src_Token, 'f': 'json', 'parentPath':'enterpriseDatabases'})
        src_httpConn.request("POST", findItems, params, headers)
        response = src_httpConn.getresponse()
        enterpriseDatabasesJson = response.read()
        enterpriseDatabaseSrc = json.loads(enterpriseDatabasesJson)
        

        params = urllib.urlencode({'token': dst_Token, 'f': 'json', 'parentPath':'enterpriseDatabases'})
        dst_httpConn.request("POST", findItems, params, headers)
        response = dst_httpConn.getresponse()
        enterpriseDatabasesJson = response.read()
        enterpriseDatabaseDst = json.loads(enterpriseDatabasesJson)
        

        srcItems = enterpriseDatabaseSrc['items']
        dstItems = enterpriseDatabaseDst['items']
        
        for item in srcItems:            
            
            try:
                dstItems.index(item)
            except:
                # Not found in dst add item
                logging.debug(str(item))
                logging.debug("Add item")
                params = urllib.urlencode({'token': dst_Token, 'f': 'json', 'item':json.dumps(item)})
                dst_httpConn.request("POST", registerItem, params, headers)
                response = dst_httpConn.getresponse()
                respJson = response.read()
                logging.debug(str(respJson))
                resp = json.loads(respJson)
                if resp['success'] == False:
                    if resp['reason'] == "EXISTS":
                        logging.debug("Assuming if it EXISTS then its ok")
                    else:
                        log.exception(respJson)            
            
        
        params = urllib.urlencode({'token': src_Token, 'f': 'json', 'parentPath':'fileShares'})
        src_httpConn.request("POST", findItems, params, headers)
        response = src_httpConn.getresponse()
        fileSharesJson = response.read()
        fileSharesSrc = json.loads(fileSharesJson)

        params = urllib.urlencode({'token': dst_Token, 'f': 'json', 'parentPath':'fileShares'})
        dst_httpConn.request("POST", findItems, params, headers)
        response = dst_httpConn.getresponse()
        fileSharesJson = response.read()
        fileSharesDst = json.loads(fileSharesJson)

        srcItems = fileSharesSrc['items']
        dstItems = fileSharesDst['items']
        for item in srcItems:
            
            try:
                dstItems.index(item)
            except:
                # Not found in dst add item
                logging.debug(str(item))
                params = urllib.urlencode({'token': dst_Token, 'f': 'json', 'item':json.dumps(item)})
                dst_httpConn.request("POST", registerItem, params, headers)
                response = dst_httpConn.getresponse()
                respJson = response.read()
                logging.debug(str(respJson))   
                resp = json.loads(respJson)
                if resp['success'] == False:
                    if resp['reason'] == "EXISTS":
                        logging.debug("Assuming if it EXISTS then its ok")
                    else:
                        log.exception(respJson)
            
            
        
    except Exception as e:
        log.exception("ERROR:" + e.message)
    finally:
        logging.debug("End Copy Data Stores")
    

def edit_service(folderName,serviceName,token,httpConn,serviceInfo,typ):

    try:
        logging.debug("Begin Edit Service")

        editService = dst_serverurl + "/arcgis/admin/services/" + folderName + "/" + serviceName + "." + typ + "/edit"
        logging.debug("editService: " + editService)

        serviceInfoJson = json.dumps(serviceInfo)
        serviceInfoJson = serviceInfoJson.replace(src_server, dst_server)

        params = urllib.urlencode({'token': token, 'f': 'json', 'service': serviceInfoJson})
        httpConn.request("POST", editService, params, headers)

        response = httpConn.getresponse()
        respJson = response.read()
        resp = json.loads(respJson)

        logging.debug(respJson)
        
        if resp['status'] == 'success':
            logging.debug("Edited")
        else:
            ok = False
            raise Exception("Edit Failed")        

    except Exception as e:
        log.exception("ERROR: " + str(e))
    finally:
        logging.debug("End Edit Service")


def create_service(folderName,serviceName,token,httpConn,serviceInfo):
    '''
    Assumes files have been copied from system input folder using rsync.  
    '''

    try:
        logging.debug("Begin Create Service")

        createService = dst_serverurl + "/arcgis/admin/services/" + folderName + "/createService"
        logging.debug("createService: " + createService)

        serviceInfoJson = json.dumps(serviceInfo)
        serviceInfoJson = serviceInfoJson.replace(src_server, dst_server)

        params = urllib.urlencode({'token': token, 'f': 'json', 'service': serviceInfoJson})
        httpConn.request("POST", createService, params, headers)

        response = httpConn.getresponse()
        respJson = response.read()
        resp = json.loads(respJson)

        logging.debug(respJson)
        
        if resp['status'] == 'success':
            logging.debug("Created")
        else:
            ok = False
            raise Exception("Create Failed")        

    except Exception as e:
        log.exception("ERROR: " + str(e))
    finally:
        logging.debug("End Delete Service")


def del_service(url,token,httpConn):
    ok = True
    try:
        logging.debug("Begin Delete Service")
        logging.debug("Path: " + url)
        
        params = urllib.urlencode({'token': token, 'f': 'json'})
        httpConn.request("POST", url + "/delete", params, headers)
        
        response = httpConn.getresponse()
        respJson = response.read()
        logging.debug(respJson)
        resp = json.loads(respJson)
        if resp['status'] == 'success':
            logging.debug("Deleted")
        else:
            ok = False
            raise Exception("Delete Failed")
    except Exception as e:
        log.exception("ERROR: " + str(e))
    finally:
        logging.debug("End Delete Service")
        return ok


def create_folder(serverurl,folderName,token,httpConn):
    ok = True
    try:
        logging.debug("Begin Create Folder")

        params = urllib.urlencode({'token': token, 'f': 'json', 'folderName': folderName})
        httpConn.request("POST", serverurl + "/arcgis/admin/services/createFolder", params, headers)

        response = httpConn.getresponse()
        respJson = response.read()
        resp = json.loads(respJson)
        if resp['status'] == 'success':
            logging.debug("Created")
        else:
            if resp['messages'][0].index("already exists") >= 0:
                logging.debug("OK folder already exists")
            else:                
                raise Exception("Create Folder Failed")
    except Exception as e:
        ok = False
        log.exception("ERROR: " + str(e))
    finally:
        logging.debug("End Create Folder")
        return ok


def del_folder(serverurl,folderName,token,httpConn):
    ok = True
    try:
        logging.debug("Begin Delete Folder")

        deleteFolder = serverurl + "/arcgis/admin/services/" + folderName + "/deleteFolder"
        logging.debug(deleteFolder)

        
        params = urllib.urlencode({'token': token, 'f': 'json'})
        httpConn.request("POST", deleteFolder, params, headers)

        
        response = httpConn.getresponse()
        respJson = response.read()
        resp = json.loads(respJson)
        if resp['status'] == 'success':
            logging.debug("Deleted")
        else: 
            raise Exception("Delete Folder Failed")
    except Exception as e:
        ok = False
        log.exception("ERROR: " + str(e))
    finally:
        logging.debug("End Deleted Folder")
        return ok

def copy_service(folderName,serviceName,src_Token,src_httpConn,dst_Token,dst_httpConn,typ):
    try:
        logging.debug("Begin Copy Service")

        path = "/arcgis/admin/services/" + folderName + "/" + serviceName + "." + typ
                                       
        logging.debug("Path: " + path)
        
        params = urllib.urlencode({'token': src_Token, 'f': 'json'})
        src_httpConn.request("POST", src_serverurl + path, params, headers)
        response = src_httpConn.getresponse()
        srcServiceInfoJson = response.read()
        srcServiceInfo = json.loads(srcServiceInfoJson)
        if (srcServiceInfo.has_key('status')):
            raise Exception("Source Service not Found")
            
        # Replacing paths
        src_urlparts = urlparse.urlparse(src_serverurl)
        dst_urlparts = urlparse.urlparse(dst_serverurl)        

        srcServiceInfoJsonMod = srcServiceInfoJson
        srcServiceInfoJsonMod = srcServiceInfoJsonMod.replace(src_serverurl,dst_serverurl)
        srcServiceInfoJsonMod = srcServiceInfoJsonMod.replace(src_urlparts.netloc,dst_urlparts.netloc)
        srcServiceInfoJsonMod = srcServiceInfoJsonMod.replace(src_home.replace("/","\\\\"),dst_home.replace("/","\\\\"))
        srcServiceInfoJsonMod = srcServiceInfoJsonMod.replace(src_home,dst_home)
        srcServiceInfoMod = json.loads(srcServiceInfoJsonMod)

        params = urllib.urlencode({'token': dst_Token, 'f': 'json'})
        dst_httpConn.request("POST", dst_serverurl + path, params, headers)
        response = dst_httpConn.getresponse()
        dstServiceInfoJson = response.read()
        dstServiceInfo = json.loads(dstServiceInfoJson)
        if (dstServiceInfo.has_key('status')):
            # The service does not exist on dst server Copy
            logging.debug("Create New service")
            create_service(folderName,serviceName,dst_Token,dst_httpConn,srcServiceInfoMod)
        else:
            # The service does exist compare
            srcTest = srcServiceInfoMod
            dstTest = dstServiceInfo
            
            #logging.debug(srcServiceInfo)
            if srcTest.has_key('extensions'):
                srcTest['extensions'].sort()                
            if dstTest.has_key('extensions'):
                dstTest['extensions'].sort()                


            # ***********************************************************************************************
            # This segement of code is useful for debugging why src and dst are identified as equal or why they are not equal
##            print srcTest
##            print dstTest
##
##            print dstTest == srcTest
##
##            for k in dstTest:
##                if dstTest[k] != srcTest[k]:
##                    print k
##                    print dstTest[k]
##                    print srcTest[k]

            # ***********************************************************************************************
            
            if srcTest == dstTest:
                # Services are the same
                logging.debug("Service are identical")
            else:
                # Service has changed
                logging.debug("Update the Service")
                edit_service(folderName,serviceName,dst_Token,dst_httpConn,srcServiceInfoMod,typ)
        

    except Exception as e:
        log.exception("ERROR: " + str(e))
    finally:
        logging.debug("End Copy Service")
    

def copy_services(src_Token,src_httpConn,src_ssl,dst_Token,dst_httpConn,dst_ssl):

    try:
        logging.debug("Begin Copy Services")

        # Service Path
        rootFoldersServicesUrl = src_serverurl + "/arcgis/admin/services"
        logging.debug(rootFoldersServicesUrl)
        # Set params
        params = urllib.urlencode({'token': src_Token, 'f': 'json'})
        # Post request
        
        src_httpConn.request("POST", rootFoldersServicesUrl, params, headers)        

        # Get the response
        response = src_httpConn.getresponse()
        
        rootFoldersServicesJson = response.read()
        

        # Convert json to python object
        rootFoldersServices = json.loads(rootFoldersServicesJson)

        services = rootFoldersServices['services']
        for service in services:
            typ = service['type']
            if typ in ['MapServer','ImageServer','GPServer']:
                # Only  process Map/Image/GP Services
                serviceName = str(service['serviceName'])

                '''
                For some reason with large number of services
                the http conn's dropped.  Refresh fixed the issue
                '''
                # Refresh http conn
                src_httpConn = None
                if src_ssl:
                    src_httpConn = httplib.HTTPSConnection(src_server)
                else:
                    src_httpConn = httplib.HTTPConnection(src_server)    

                # Refresh http conn
                dst_httpConn = None
                if dst_ssl:
                    dst_httpConn = httplib.HTTPSConnection(dst_server)
                else:
                    dst_httpConn = httplib.HTTPConnection(dst_server)
                    
                copy_service("",serviceName,src_Token,src_httpConn,dst_Token,dst_httpConn,typ)
            
        folders = rootFoldersServices['folders']
        for folder in folders:
            if folder != 'System' and folder != 'Utilities':
                # Skip System and Utilities folders

                if create_folder(dst_serverurl,folder,dst_Token,dst_httpConn):
                    # Create Services for the folder

                    # Output folder
                    folderPath = rootFoldersServicesUrl + "/" + folder
                    src_httpConn.request("POST", folderPath, params, headers)
                
                    response = src_httpConn.getresponse()
                    folderInfoJson = response.read()

                    folderInfo = json.loads(folderInfoJson)

                    services = folderInfo['services']
                
                    for service in services:
                        typ = service['type']
                        if typ in ['MapServer','ImageServer','GPServer']:
                            serviceName = str(service['serviceName'])

                            '''
                            For some reason with large number of services
                            the http conn's dropped.  Refresh fixed the issue
                            '''
                            # Refresh http conn
                            src_httpConn = None
                            if src_ssl:
                                src_httpConn = httplib.HTTPSConnection(src_server)
                            else:
                                src_httpConn = httplib.HTTPConnection(src_server)    

                            # Refresh http conn
                            dst_httpConn = None
                            if dst_ssl:
                                dst_httpConn = httplib.HTTPSConnection(dst_server)
                            else:
                                dst_httpConn = httplib.HTTPConnection(dst_server)
                                
                            copy_service(folder,serviceName,src_Token,src_httpConn,dst_Token,dst_httpConn,typ)

    except Exception as e:
        log.exception("ERROR: " + str(e) )
    finally:
        logging.debug("End Copy Services")
        try:
            dst_httpConn.close()
        except:
            logging.info("Already Closed")
        try:
            src_httpConn.close()
        except:
            logging.info("Already Closed")           


def service_exists(serverurl,httpConn,token,serviceName,folderName,typ):
    exists = False
    try:
        logging.debug("Begin Service Exists")
        serviceExists = serverurl + "/arcgis/admin/services/exists"
        logging.debug(serviceExists)
        logging.debug(serviceName + "," + folderName + "," + typ)

        params = urllib.urlencode({'token': token, 'f': 'json', 'folderName': folderName, 'serviceName': serviceName, 'type': typ})
        httpConn.request("POST", serviceExists, params, headers)
        

        response = httpConn.getresponse()
        
        respJson = response.read()
        
        resp = json.loads(respJson)        
        
        if resp['exists'] == True:
            exists = True
        
    except Exception as e:
        # On Exception Assume the Service Exists
        exists = True
        log.exception("ERROR: " + str(e))
    finally:
        
        logging.debug("End Service Exists")
        return exists


def remove_deleted_services(src_Token,src_httpConn,dst_Token,dst_httpConn):

    try:
        logging.debug("Begin Removed Deleted Services")

        # Service Path
        rootFoldersServicesUrl = dst_serverurl + "/arcgis/admin/services"
        logging.debug(rootFoldersServicesUrl)
        # Set params
        params = urllib.urlencode({'token': dst_Token, 'f': 'json'})
        # Post request
        
        dst_httpConn.request("POST", rootFoldersServicesUrl, params, headers)        

        # Get the response
        response = dst_httpConn.getresponse()
        
        rootFoldersServicesJson = response.read()
        
        # Convert json to python object
        rootFoldersServices = json.loads(rootFoldersServicesJson)

        services = rootFoldersServices['services']
        for service in services:
            typ = service['type']
            if typ in ['MapServer','ImageServer','GPServer']:
                # Only  process Map Services
                serviceName = str(service['serviceName'])
                if not service_exists(src_serverurl,src_httpConn,src_Token,serviceName,"",typ):
                    logging.debug("Delete service: " + serviceName)
                    url = dst_serverurl + "/arcgis/admin/services/" + serviceName + "." + typ 
                    del_service(url, dst_Token, dst_httpConn)
            
        folders = rootFoldersServices['folders']
        for folder in folders:
            if folder != 'System' and folder != 'Utilities':
                # Skip System and Utilities folders

                if not service_exists(src_serverurl,src_httpConn,src_Token,"",folder,""):
                    # Folder doesn't exist on source
                    logging.debug("Delete folder")
                    del_folder(dst_serverurl,folder,dst_Token,dst_httpConn)
                else:
                    # Output folder
                    folderPath = rootFoldersServicesUrl + "/" + folder
                    dst_httpConn.request("POST", folderPath, params, headers)
                
                    response = dst_httpConn.getresponse()
                    folderInfoJson = response.read()

                    folderInfo = json.loads(folderInfoJson)

                    services = folderInfo['services']
                
                    for service in services:
                        typ = service['type']
                        if typ in ['MapServer','ImageServer','GPServer']:
                            serviceName = str(service['serviceName'])
                            if not service_exists(src_serverurl,src_httpConn,src_Token,serviceName,folder,typ):
                                logging.debug("Delete service: " + serviceName)
                                url = dst_serverurl + "/arcgis/admin/services/" + folder + "/" + serviceName + "." + typ 
                                del_service(url, dst_Token, dst_httpConn)                                

    except Exception as e:
        log.exception("ERROR: " + str(e) )
    finally:
        logging.debug("End Removed Deleted  Services")




if __name__ == '__main__':

    try:
        st = time.time()
        import_tm = int(st)

        log_file = os.path.join(log_file_basename + "_" + str(import_tm) + ".log")
        if log_to_file:
            logging.basicConfig(level=log_level,
                                format='%(asctime)s %(levelname)-8s %(message)s',
                                datefmt='%m-%d %H:%M:%S',
                                filename=log_file)
        else:
            logging.basicConfig(level=log_level,
                                format='%(asctime)s %(levelname)-8s %(message)s',
                                datefmt='%m-%d %H:%M:%S')

        dtg = time.strftime("%m%d%Y_%H%M%S")
        logging.info("*******************************************")
        logging.info("Started@: " + dtg)    


        # Parse the provide server url
        src_urlparts = urlparse.urlparse(src_serverurl)

        src_server = src_urlparts.netloc

        src_ssl = False
        if src_urlparts.scheme == 'https':
            src_ssl = True
        
        # Get a token
        src_Token = getToken(src_user, src_pw, src_server, src_ssl)
        logging.debug("SRC Token: " + src_Token)

        # Create http conn
        src_httpConn = None
        if src_ssl:
            src_httpConn = httplib.HTTPSConnection(src_server)
        else:
            src_httpConn = httplib.HTTPConnection(src_server)        


        # Parse the provide server url
        dst_urlparts = urlparse.urlparse(dst_serverurl)

        dst_server = dst_urlparts.netloc

        dst_ssl = False
        if dst_urlparts.scheme == 'https':
            dst_ssl = True
        
        # Get a token
        dst_Token = getToken(dst_user, dst_pw, dst_server, dst_ssl)
        logging.debug("DST Token: " + dst_Token)

        # Create http conn
        dst_httpConn = None
        if dst_ssl:
            dst_httpConn = httplib.HTTPSConnection(dst_server)
        else:
            dst_httpConn = httplib.HTTPConnection(dst_server)        

        remove_deleted_services(src_Token,src_httpConn,dst_Token,dst_httpConn)
        
        copy_data_stores(src_Token,src_httpConn,dst_Token,dst_httpConn)

        copy_services(src_Token,src_httpConn,src_ssl,dst_Token,dst_httpConn,dst_ssl)



    except Exception as e:
        log.exception("ERROR: " + str(e))
        
    finally:
        logging.info("Ended@: " + time.strftime("%m%d%Y_%H%M%S"))
        try:
            dst_httpConn.close()
        except:
            logging.info("Already Closed")
        try:
            src_httpConn.close()
        except:
            logging.info("Already Closed")            
            
        
