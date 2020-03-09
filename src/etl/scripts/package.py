from zipfile import ZipFile

zipObj = ZipFile('..\\artifact\\wwld_etl.zip', 'w')

zipObj.write('..\\pipeline\\wwld.py',arcname='pipeline\\wwld.py')
zipObj.write('..\\pipeline\\plss.py',arcname='pipeline\\plss.py')
zipObj.write('..\\pipeline\\penalties.py',arcname='pipeline\\penalties.py')
zipObj.write('..\\pipeline\\paymentsOld.py',arcname='pipeline\\paymentsOld.py')
zipObj.write('..\\pipeline\\payments.py',arcname='pipeline\\payments.py')
zipObj.write('..\\pipeline\\les.py',arcname='pipeline\\les.py')
zipObj.write('..\\pipeline\\jp.py',arcname='pipeline\\jp.py')
zipObj.write('..\\pipeline\\ists.py',arcname='pipeline\\ists.py')
zipObj.write('..\\pipeline\\futil.py',arcname='pipeline\\futil.py')
zipObj.write('..\\pipeline\\earnings.py',arcname='pipeline\\earnings.py')
zipObj.write('..\\pipeline\\data_file.py',arcname='pipeline\\data_file.py')
zipObj.write('..\\pipeline\\__init__.py',arcname='pipeline\\__init__.py')

zipObj.write('..\\pipeline\\config.json',arcname='config.json')
zipObj.write('..\\pipeline\\config_prod.json',arcname='config_prod.json')
zipObj.write('..\\scripts\\install.ps1',arcname='install.ps1')
zipObj.write('..\\scripts\\start.ps1',arcname='start.ps1')
zipObj.write('..\\requirements.txt',arcname='requirements.txt')

zipObj.close()




