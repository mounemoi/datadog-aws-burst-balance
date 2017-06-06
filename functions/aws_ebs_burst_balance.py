# -*- coding: utf-8 -*-
from boto3.session import Session
import time
import datetime

REGION      = 'ap-northeast-1'
METRIC_NAME = 'test.aws.ebs.burst_balance'


def handle(event, context):
    session = Session(region_name=REGION)

    ec2 = session.client('ec2')
    next_token = ''
    ebs_list = []
    while True:
        instances = ec2.describe_instances(
            Filters=[
                { 'Name': 'instance-state-name', 'Values': ['running'] },
            ],
            MaxResults=100,
            NextToken=next_token,
        )

        for reservation in instances['Reservations']:
            for instance in reservation['Instances']:

                name = ''
                if 'Tags' in instance:
                    for tag in instance['Tags']:
                        if tag['Key'] == 'Name':
                            name = tag['Value']
                            break

                if 'BlockDeviceMappings' in instance:
                    for ebs in instance['BlockDeviceMappings']:
                        volume_id = ebs['Ebs']['VolumeId']
                        ebs_list.append({ 'name': name, 'volume_id': volume_id })

        if 'NextToken' in instances:
            next_token = instances['NextToken']
        else:
            break

    cloudwatch = session.client('cloudwatch')
    for ebs in ebs_list:
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/EBS',
            MetricName='BurstBalance',
            Dimensions=[ { 'Name': 'VolumeId', 'Value': ebs['volume_id'] } ],
            StartTime=datetime.datetime.utcnow() - datetime.timedelta(minutes=30),
            EndTime=datetime.datetime.utcnow(),
            Period=60,
            Statistics=[ 'Minimum' ],
            Unit='Percent',
        )

        if len(response['Datapoints']) != 0:
            datapoint = sorted(response['Datapoints'], key=lambda k: k['Timestamp'])[-1]

            timestamp = int(time.mktime(datapoint['Timestamp'].timetuple())) + 9 * 60 * 60 # JST
            value     = datapoint['Minimum']

            send_metric(timestamp, ebs['name'], ebs['volume_id'], value)
        else:
            print('{name} : {volume_id} : failure to get'.format(**ebs))


def send_metric(timestamp, instance_name, volume_id, value):
    tags = [
        'ae-name:{}'.format(instance_name),
        'ae-volume-id:{}'.format(volume_id),
    ]
    print('MONITORING|{0}|{1}|{2}|{3}|#{4}'.format(timestamp, value, 'count', METRIC_NAME, ','.join(tags)))
