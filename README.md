# cloud-composer
##### *This project is a continuation of the [Cloud-Storage-Pubsub](https://github.com/Ashwini-Analytics/Cloud-Storage-Pubsub) project*


### Create an composer enviornment

#### Before you begin:
- Create a service account and give the read and write permission of the cloud storage, user and create permission of big query and general access roles. 

1. In the Google Cloud Console, go to the Create environment page.
2. Go to Create environment
3. In the Name field, enter example-environment.
4. In the Location drop-down list, select a region for the Cloud Composer environment. 
5. select the service account that you have created earlier. 
6. select machine type as n1-standard1
7. composer version 1.7.1 and airflow version 1.10.15
8. select the GKE cluster.
9. click on create, it will take few minutes to create. 


### After creation of the composer
- upload the capstone_dag.py file in the composer's DAG folder.
- wait for few seconds and then your dag file will start running. 




