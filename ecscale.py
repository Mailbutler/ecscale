import boto3
import datetime
import os

SCALE_IN_CPU_TH = os.environ['SCALE_IN_CPU_TH'] if 'SCALE_IN_CPU_TH' in os.environ else 25
SCALE_IN_MEM_TH = os.environ['SCALE_IN_MEM_TH'] if 'SCALE_IN_MEM_TH' in os.environ else 75
FUTURE_MEM_TH = os.environ['FUTURE_MEM_TH'] if 'FUTURE_MEM_TH' in os.environ else 85
ECS_AVOID_STR = os.environ['ECS_AVOID_STR'] if 'ECS_AVOID_STR' in os.environ else 'awseb'
logline = {}

def clusters(ecsClient):
    # Returns an iterable list of cluster names
    response = ecsClient.list_clusters()
    if not response['clusterArns']:
        print 'No ECS cluster found'
        return

    return [cluster for cluster in response['clusterArns'] if ECS_AVOID_STR not in cluster]


def cluster_memory_reservation(cwClient, clusterName):
    # Return cluster mem reservation average per 5 minutes cloudwatch metric
    try:
        response = cwClient.get_metric_statistics(
            Namespace='AWS/ECS',
            MetricName='MemoryReservation',
            Dimensions=[
                {
                    'Name': 'ClusterName',
                    'Value': clusterName
                },
            ],
            StartTime=datetime.datetime.utcnow() - datetime.timedelta(seconds=300),
            EndTime=datetime.datetime.utcnow(),
            Period=300,
            Statistics=['Average']
        )
        return response['Datapoints'][0]['Average']

    except Exception:
        logger({'ClusterMemoryError': 'Could not retrieve mem reservation for {}'.format(clusterName)})


def find_asg(clusterName, asgData):
    # Returns auto scaling group resourceId based on name
    for asg in asgData['AutoScalingGroups']:
        for tag in asg['Tags']:
            if tag['Key'] == 'aws:cloudformation:stack-name':
                if clusterName in tag['Value']:
                    return tag['ResourceId']

    else:
        logger({'ASGError': 'Auto scaling group for {} not found'.format(clusterName)})


def ec2_avg_cpu_utilization(clusterName, asgData, cwclient):
    asg = find_asg(clusterName, asgData)
    response = cwclient.get_metric_statistics(
        Namespace='AWS/EC2',
        MetricName='CPUUtilization',
        Dimensions=[
            {
                'Name': 'AutoScalingGroupName',
                'Value': asg
            },
        ],
        StartTime=datetime.datetime.utcnow() - datetime.timedelta(seconds=300),
        EndTime=datetime.datetime.utcnow(),
        Period=300,
        Statistics=['Average']
    )
    return response['Datapoints'][0]['Average']


def asg_on_min_state(clusterName, asgData, asgClient):
    asg = find_asg(clusterName, asgData)
    for sg in asgData['AutoScalingGroups']:
        if sg['AutoScalingGroupName'] == asg:
            if sg['MinSize'] == sg['DesiredCapacity']:
                return True

    return False

def asg_scalable_instance_count(clusterName, asgData, asgClient):
    asg = find_asg(clusterName, asgData)
    for sg in asgData['AutoScalingGroups']:
        if sg['AutoScalingGroupName'] == asg:
            return sg['DesiredCapacity'] - sg['MinSize']

    return 0


def empty_instances(clusterArn, activeContainerDescribed):
    # returns a object of empty instances in cluster
    empty_instances = {}

    for inst in activeContainerDescribed['containerInstances']:
        if inst['runningTasksCount'] == 0 and inst['pendingTasksCount'] == 0:
            empty_instances.update({inst['ec2InstanceId']: inst['containerInstanceArn']})

    return empty_instances


def draining_instances(clusterArn, drainingContainerDescribed):
    # returns an object of draining instances in cluster
    draining_instances = {}

    for inst in drainingContainerDescribed['containerInstances']:
        draining_instances.update({inst['ec2InstanceId']: inst['containerInstanceArn']})

    return draining_instances


def terminate_decrease(instanceId, asgClient):
    # terminates an instance and decreases the desired number in its auto scaling group
    # [ only if desired > minimum ]
    try:
        response = asgClient.terminate_instance_in_auto_scaling_group(
            InstanceId=instanceId,
            ShouldDecrementDesiredCapacity=True
        )
        logger({'Action': 'Terminate', 'Message': response['Activity']['Cause']})

    except Exception as e:
        logger({'Error': e})


def scale_in_instance(clusterArn, activeContainerDescribed):
    # iterates over hosts, finds the least utilized:
    # The most under-utilized memory and minimum running tasks
    # return instance obj {instanceId, runningInstances, containerinstanceArn}
    instanceToScale = {'id': '', 'running': 0, 'freemem': 0}
    for inst in activeContainerDescribed['containerInstances']:
        for res in inst['remainingResources']:
            if res['name'] == 'MEMORY':
                if res['integerValue'] > instanceToScale['freemem']:
                    instanceToScale['freemem'] = res['integerValue']
                    instanceToScale['id'] = inst['ec2InstanceId']
                    instanceToScale['running'] = inst['runningTasksCount']
                    instanceToScale['containerInstanceArn'] = inst['containerInstanceArn']

                elif res['integerValue'] == instanceToScale['freemem']:
                    # Two instances with same free memory level, choose the one with less running tasks
                    if inst['runningTasksCount'] < instanceToScale['running']:
                        instanceToScale['freemem'] = res['integerValue']
                        instanceToScale['id'] = inst['ec2InstanceId']
                        instanceToScale['running'] = inst['runningTasksCount']
                        instanceToScale['containerInstanceArn'] = inst['containerInstanceArn']
                break

    logger({'Scale candidate': '{} with free {}'.format(instanceToScale['id'], instanceToScale['freemem'])})
    return instanceToScale


def running_tasks(instanceId, containerDescribed):
    # return a number of running tasks on a given ecs host
    for inst in containerDescribed['containerInstances']:
        if inst['ec2InstanceId'] == instanceId:
            return int(inst['runningTasksCount']) + int(inst['pendingTasksCount'])


def drain_instance(containerInstanceId, ecsClient, clusterArn):
    # put a given ec2 into draining state
    try:
        ecsClient.update_container_instances_state(
            cluster=clusterArn,
            containerInstances=[containerInstanceId],
            status='DRAINING'
        )

    except Exception as e:
        logger({'DrainingError': e})


def future_reservation(activeContainerDescribed, clusterMemReservation):
    # If the cluster were to scale in an instance, calculate the effect on mem reservation
    # return cluster_mem_reserve*num_of_ec2 / num_of_ec2-1
    numOfEc2 = len(activeContainerDescribed['containerInstances'])
    if numOfEc2 > 1:
        futureMem = (clusterMemReservation*numOfEc2) / (numOfEc2-1)
    else:
        return 100

    print '*** Current: {} | Future : {}'.format(clusterMemReservation, futureMem)

    return futureMem


def asg_scaleable(asgData, clusterName):
    asg = find_asg(clusterName, asgData)
    for group in asgData['AutoScalingGroups']:
        if group['AutoScalingGroupName'] == asg:
            return True if group['MinSize'] < group['DesiredCapacity'] else False
    else:
        print 'Cannot find AutoScalingGroup to verify scaleability'
        return False


def retrieve_cluster_data(ecsClient, cwClient, asgClient, cluster):
    clusterName = cluster.split('/')[1]
    print '*** {} ***'.format(clusterName)
    activeContainerInstances = ecsClient.list_container_instances(cluster=cluster, status='ACTIVE')
    clusterMemReservation = cluster_memory_reservation(cwClient, clusterName)

    if activeContainerInstances['containerInstanceArns']:
        activeContainerDescribed = ecsClient.describe_container_instances(cluster=cluster, containerInstances=activeContainerInstances['containerInstanceArns'])
    else:
        print 'No active instances in cluster'
        return False
    drainingContainerInstances = ecsClient.list_container_instances(cluster=cluster, status='DRAINING')
    if drainingContainerInstances['containerInstanceArns']:
        drainingContainerDescribed = ecsClient.describe_container_instances(cluster=cluster, containerInstances=drainingContainerInstances['containerInstanceArns'])
        drainingInstances = draining_instances(cluster, drainingContainerDescribed)
    else:
        drainingInstances = {}
        drainingContainerDescribed = []
    emptyInstances = empty_instances(cluster, activeContainerDescribed)

    dataObj = {
        'clusterName': clusterName,
        'clusterMemReservation': clusterMemReservation,
        'activeContainerDescribed': activeContainerDescribed,
        'drainingInstances': drainingInstances,
        'emptyInstances': emptyInstances,
        'drainingContainerDescribed': drainingContainerDescribed
    }

    return dataObj


def logger(entry, action='log'):
    # print log as one-line json from cloudwatch integration
    if action == 'log':
        global logline
        logline.update(entry)
    elif action == 'print':
        print entry


def main(run='normal'):
    ecsClient = boto3.client('ecs')
    cwClient = boto3.client('cloudwatch')
    asgClient = boto3.client('autoscaling')
    asgData = asgClient.describe_auto_scaling_groups()
    clusterList = clusters(ecsClient)

    for cluster in clusterList:
        ########### Cluster data retrival ##########
        clusterData = retrieve_cluster_data(ecsClient, cwClient, asgClient, cluster)
        if not clusterData:
            continue
        else:
            clusterName = clusterData['clusterName']
            clusterMemReservation = clusterData['clusterMemReservation']
            activeContainerDescribed = clusterData['activeContainerDescribed']
            drainingInstances = clusterData['drainingInstances']
            emptyInstances = clusterData['emptyInstances']
            ########## Cluster scaling rules ###########

        if asg_on_min_state(clusterName, asgData, asgClient):
            print '{}: in Minimum state, skipping'.format(clusterName)
            continue

        scalableCount = asg_scalable_instance_count(clusterName, asgData, asgClient) - len(drainingInstances)
        print '{0}: {1} instances can be scaled'.format(clusterName, scalableCount)

        if (scalableCount > 0 and clusterMemReservation < FUTURE_MEM_TH and
           future_reservation(activeContainerDescribed, clusterMemReservation) < FUTURE_MEM_TH):
            # Future memory levels allow scale
            if emptyInstances.keys():
                # There are empty instance
                for instanceId, containerInstId in emptyInstances.iteritems():
                    if scalableCount > 0:
                        if run == 'dry':
                            print 'Would have drained {}'.format(instanceId)
                        else:
                            print 'Draining empty instance {}'.format(instanceId)
                            drain_instance(containerInstId, ecsClient, cluster)
                        scalableCount -= 1
                    else:
                        print 'Minimum state reached. Cannot scale another instance.'
                        break

            if (clusterMemReservation < SCALE_IN_MEM_TH):
                # Cluster mem reservation level requires scale
                if (ec2_avg_cpu_utilization(clusterName, asgData, cwClient) < SCALE_IN_CPU_TH):
                    if scalableCount > 0:
                        instanceToScale = scale_in_instance(cluster, activeContainerDescribed)['containerInstanceArn']
                        if run == 'dry':
                            print 'Would have scaled {}'.format(instanceToScale)
                        else:
                            print 'Draining least utilized instanced {}'.format(instanceToScale)
                            drain_instance(instanceToScale, ecsClient, cluster)
                        scalableCount -= 1
                    else:
                        print 'Minimum state reached. Cannot scale another instance.'
                else:
                    print 'CPU higher than TH, cannot scale'


        if drainingInstances.keys():
            # There are draining instances to terminate
            for instanceId, containerInstId in drainingInstances.iteritems():
                if not running_tasks(instanceId, clusterData['drainingContainerDescribed']):
                    if run == 'dry':
                        print 'Would have terminated {}'.format(instanceId)
                    else:
                        print 'Terminating draining instance with no containers {}'.format(instanceId)
                        terminate_decrease(instanceId, asgClient)
                else:
                    print 'Draining instance not empty'

        print '***'
        print logline

def lambda_handler(event, context):
    runType = 'dry' if 'DRY_RUN' in os.environ else 'normal'
    main(run=runType)


if __name__ == '__main__':
    # lambda_handler({}, '')
    main()
