# -*- coding: utf-8 -*-
from checks import AgentCheck
from boto3.session import Session
import datetime


class EBSBurstBalance(AgentCheck):
    def check(self, config):
        if 'region' not in config:
            self.log.error('no region')
            return

        session = Session(region_name=config['region'])

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
                Period=300,
                Statistics=[ 'Minimum' ],
                Unit='Percent',
            )

            if len(response['Datapoints']) != 0:
                self.gauge(
                    self.init_config.get('metrics_name', 'aws.ebs.burst_balance'),
                    sorted(response['Datapoints'], key=lambda k: k['Timestamp'])[-1]['Minimum'],
                    tags=[ 'ae-name:{name}'.format(**ebs), 'ae-volume-id:{volume_id}'.format(**ebs) ],
                )
            else:
                self.log.info('{name} : {volume_id} : failure to get'.format(**ebs))
