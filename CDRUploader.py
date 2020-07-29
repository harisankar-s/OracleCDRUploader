
#!/bin/env python
import os,fnmatch
import sys
import  datetime
import subprocess
import time
from datetime import  datetime,date,timedelta
import SmsCDRProperties
import shutil


#my_path = "/usr/bin/python:/usr/bin/python2:/app/oracle/product/12.2.0/client_1:/app/oracle/product/12.2.0/client_1/bin"
#my_env = os.environ.copy()
#my_env["PATH"] = my_path + ":" + my_env["PATH"]

#Function to get timestamp
def getTime():
    timevar = datetime.now()
    starttime = timevar.strftime("%c.%f")
    return  starttime

#Function to find the filename from the filepath
def find(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result

#Function for CDR Upload
def cdrFileDBUploader( dbserverIP, dbserverPort, dbserverUserName, dbserverPassword, dbbaseName):
    fileUploaddStatus=0
    retryCnt = 0
    global db1UploadStatus
    db1UploadStatus = 'false'
    while ((fileUploaddStatus == 0)) & ((SmsCDRProperties.cdrUploadRetryCnt >= retryCnt)):
        print('Loading CDR File')
        bashCommand = [('sqlldr userid=%s' % SmsCDRProperties.Loaderuserid + ' control=%s' % SmsCDRProperties.Loaderctlname + ' bad=%s' % SmsCDRProperties.cdr_reject_path+str('/')+file3+str('.bad') + ' log=%s' % SmsCDRProperties.Loaderlog)]
        #process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        print bashCommand
        process = subprocess.Popen(bashCommand,shell=True, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        output, error = process.communicate()

        print output,error
        os.remove(SmsCDRProperties.Loaderctlname)
        fileUploaddStatus=1
        if(fileUploaddStatus == 1):
            db1UploadStatus = 'true'
            print(getTime(),"Successfully Loaded the CDR From File",filename ,"Having Size" ,fileSize," into DB With IP",dbserverIP)
        else:
            retryCnt = retryCnt + 1
            print(getTime(), "WARNING: Uploading of CDR File", filename, "into DB with IP",dbserverIP,"Failed File Upload - Retry Counter = ",retryCnt)
            if ((cdrUploadRetryCnt < retryCnt)):
                print(getTime()," ERROR: File Upload Retry Count Value Exceeds - Retry Counter = ",retryCnt, "Uploading of File ",filename, dbserverIP ,"into DB  Stopping Forcefully.")
            db1UploadStatus = 'false'
            time.sleep(1)

#Body of the Upload Script

thisfilename = os.path.basename(__file__)
#cmd = ('pgrep -f %s' %  thisfilename)
cmd = ['pgrep -f .*python.*'+str(thisfilename)]

print("Checking Script is already running or not",cmd)
process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
my_pid, err = process.communicate()
print my_pid, err
pidlength = my_pid.splitlines()

if len(pidlength) >2:
   print("++++++++++++++++++++++Already Running Exiting the new Instance.++++++++++++++++++++++++")
   exit()
else:
  print("************Script is not Running!!!...Starting CDR Upload **************************************************************************** ")
  propertyFilepath = os.getcwd()
  print "Getting the Details From Properties File Located in the Path :" ,propertyFilepath , getTime()

  print getTime()," ***********************************CDR UPLOADING PROCESS STARTING With the Following Parameters*************************"
  print "Notification Table Prefix        = ",SmsCDRProperties.cdr_table_prefix
  print "Table Columns                    = ",SmsCDRProperties.cdr_columns
  print "Cdr File Path                    = ",SmsCDRProperties.cdr_path
  print "SMS CDR Backup Path              = ",SmsCDRProperties.cdr_backup_path
  print "SMS CDR File Prefix              = ",SmsCDRProperties.cdr_file_prefix
  print "SMS CDR File Postfix             = ",SmsCDRProperties.cdr_file_postfix
  print "CDR Upload Retry Count           = ",SmsCDRProperties.cdrUploadRetryCnt

  fileCount=0
  today_date=datetime.today().strftime('%d-%m-%Y')
  yesterday=date.today() - timedelta(days=1)
  ystrday_date=yesterday.strftime('%d-%m-%Y')
  my_list=find('*.txt',SmsCDRProperties.cdr_path)


  for filename in  my_list:
      dbInstanceToken=0
      db1UploadStatus = 'false'
      fileCount = fileCount + 1
      tmpFileName = filename
      file1 = filename.rsplit("_", 1)[0]
      formatfilename = "'{0}'".format(filename)
      file2 = os.path.basename(filename)
      file3 = os.path.splitext(file2)[0]

      #BL01AUDIT_20200528161213_4.txt-----Filename format
      date = filename.rsplit("_", 2)[1]
      year = filename.split('_')[1][:4]
      month = filename.split('_')[1][4:6]
      day = filename.split('_')[1][6:8]
      hour = filename.split('_')[1][8:10]
      fileSize =os.stat(filename).st_size
      if(SmsCDRProperties.cdr_table_type == 'DAILY'):
          tablename = SmsCDRProperties.cdr_table_prefix+str(day)
      elif(SmsCDRProperties.cdr_table_type == 'MONTHLY'):
          tablename = SmsCDRProperties.cdr_table_prefix+str(month)
      else:
          tablename = SmsCDRProperties.cdr_table_prefix

      #Python Recommendation is to give long strings in separate lines as given  below
      SqlldrLoadDataGenericQueryString = ('load data CHARACTERSET UTF8  infile '
      '{} '
      '\nappend  into  table  %s  fields  terminated by  "#$$#" TRAILING NULLCOLS(REQUEST_ID,SUB_TRANS_ID,TIMESTAMP TIMESTAMP "dd-mm-yy hh24:mi:ss.ff",'
      'SOURCE_SYSTEM,KEYWORD,CUSTOMER_NUMBER,ACCOUNT_NUMBER,SERVICE_NUMBER,CUSTOMER_SEGMENT,CATEGORY,TEMPLATE_ID,NOTIFICATION_LEVEL,DEFAULT_PREFERENCE_ID,'
      'CATEGORY_PREFERENCE_ID,CHANNEL,PREFERRED_CONTACT_NUMBER,LANGUAGE,FROM_TIME,TO_TIME,TRANSACTION_MODE,'
      'MESSAGE_SENT_TIME TIMESTAMP "yyyy-mm-dd hh24:mi:ss.ff",SHORT_CODE,FINAL_MSG,ESME_ID,STATUS_CODE,STATUS_DESC,ERROR_CODE,'
      'ERROR_DESC,ESME_STATUS,ESME_RESP,ELAPSED_TIME)' % tablename).format(formatfilename)
      cdrDate = year + str('-') + month + str('-') + day
      print(getTime(), "========================CDR Processing of File", filename,"Started===========",)

      #Writing CTL file for loading the data
      with open(SmsCDRProperties.Loaderctlname, "w") as f:
          f.write(SqlldrLoadDataGenericQueryString)

      #Calling Function for CDR Upload
      cdrFileDBUploader(SmsCDRProperties.db_ip, SmsCDRProperties.db_port, SmsCDRProperties.db_user, SmsCDRProperties.db_pwd, SmsCDRProperties.db_name)
      print(getTime()," CDR File ::", filename," UPLOAD STATUS - DB01 :: ",db1UploadStatus)

      if(db1UploadStatus):
          print(getTime(),"Processing of ",filename," is Completed")
          file_date = day+str('_')+month+str('_')+year
          file_dir = SmsCDRProperties.cdr_backup_path+str('/')+file_date
          print(file_dir,file_date,today_date)

          if(file_date == today_date):
              try:
                  os.makedirs(file_dir)
              except OSError:
                  if not os.path.isdir(file_dir):
                      raise
              print(getTime(),"Moving",filename,"from",SmsCDRProperties.cdr_path,"to",file_dir)
              dst_filename = os.path.join(file_dir, os.path.basename(filename));
              print dst_filename
              shutil.move(filename,dst_filename )
          else:
              try:
                  os.makedirs(file_dir)
              except OSError:
                  if not os.path.isdir(file_dir):
                      raise
              print(getTime(), "Moving", filename, "from", SmsCDRProperties.cdr_path, "to", file_dir)
              dst_filename = os.path.join(file_dir, os.path.basename(filename));
              print dst_filename
              shutil.move(filename,dst_filename )
              #subprocess.call(['zip', '-r', file_dir + str('zip'), file_dir])
              #shutil.rmtree(file_dir)
      else:
          print(getTime(),"WARNING: CDR File Upload Failed in the DB with IP ",SmsCDRProperties.db_ip)
          print(getTime(),"Moving File",filename,"into Backup  Path",SmsCDRProperties.cdr_reject_path)


print(getTime()," Number of Files Processed = " ,fileCount)
if(fileCount == 0):
    print(getTime(),"There is No CDR Files to Process.")
print(getTime(), "****************************************************CDR UPLOADING PROCESS COMPLETED*************************************")
