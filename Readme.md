The purpose of these scripts is to request satelite tracking/telemetry data files from the argos.cls server, compare that data with existing data in a database, and import any new records. We run this script on a daily basis to pull the data from the server and ingest it to our database. 

Running this script on a schedule ensure it is completed on a regular basis.

Other users would need to modify the script to push to their own databases and adjust the field names as appropriate. 

Note that this repo includes a **SAMPLE** secrets file where ther username, password, and program ID should be added and the database string should be altered as well. 

