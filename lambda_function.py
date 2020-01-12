from datetime import datetime,timezone
import pytz
import boto3

print('Loading function')

client = boto3.client('emr')
terminationTag = 'terminateIfIdleForMins'

def dateDiffMins(start_date, end_date):
    start = start_date.replace(tzinfo=timezone.utc)
    end = end_date.replace(tzinfo=timezone.utc)
    return int(((end - start).total_seconds())/60)
        
def getTerminationTag(cluster_id):
    cluster_details = client.describe_cluster(
    ClusterId=cluster_id
    )
    tags = cluster_details['Cluster']['Tags']
    for tag in tags:
        if(tag['Key'] == terminationTag):
            return tag['Value']
    return 0

def getLastRunStepTime(cluster_id):
    steps_list = client.list_steps( ClusterId = cluster_id)
    #setting latest_step to 0 i.e 1970 jan 01
    latest_step = pytz.utc.localize(datetime.fromtimestamp(0))
    for step in steps_list['Steps']:
        endDate = step['Status']['Timeline']['EndDateTime']
        if(endDate > latest_step):
            latest_step = endDate
    return latest_step

def terminateCluster(cluster_id):
    print(f"terminating cluster {cluster_id}")
    response = client.terminate_job_flows(JobFlowIds=[cluster_id])
    print(response)

def lambda_handler(event, context):
    response = client.list_clusters(
        ClusterStates=[
            'WAITING'
        ]
     )
    
    
    for cluster in response['Clusters']:
        cluster_id = cluster['Id']
        cluster_name = cluster['Name']
        terminationTag = getTerminationTag(cluster_id )
        if( terminationTag != 0):
            now = pytz.utc.localize(datetime.now())
            terminationTag = int(terminationTag)
            lastStepTime = getLastRunStepTime(cluster_id)
            print(f"Processing cluster  {cluster_name} : {cluster_id} with terminationTag = {terminationTag} and last step run at :  {lastStepTime}")
            if(lastStepTime != pytz.utc.localize(datetime.fromtimestamp(0))):
                minsSinceRun = dateDiffMins( lastStepTime, now)
                print(f"latest run step for {cluster_name} : {cluster_id} was {minsSinceRun} minutes ago")
                if (minsSinceRun > terminationTag):
                    terminateCluster(cluster_id)