'''
Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
'''

# delete_old_snapshots_dest_rds
# This lambda function will delete manual RDS snapshots that have expired in the region specified in the environment variable DEST_REGION, and according to the environment variables SNAPSHOT_PATTERN and RETENTION_DAYS.
# Set SNAPSHOT_PATTERN to a regex that matches your RDS Instance identifiers
# Set DEST_REGION to the destination AWS region
# Set RETENTION_DAYS to the amount of days snapshots need to be kept before deleting
import boto3
import time
import os
import logging
from datetime import datetime, date, timedelta
import re
from snapshots_tool_utils import *


# Initialize everything
DEST_REGION = os.getenv('DEST_REGION', os.getenv('AWS_DEFAULT_REGION')).strip()
LOGLEVEL = os.getenv('LOG_LEVEL', 'ERROR').strip()
PATTERN = os.getenv('SNAPSHOT_PATTERN', 'ALL_SNAPSHOTS')
RETENTION_DAYS = int(os.getenv('RETENTION_DAYS'))
RETENTION_MONTHS = 1
TIMESTAMP_FORMAT = '%Y-%m-%d-%H-%M'

logger = logging.getLogger()
logger.setLevel(LOGLEVEL.upper())

def lambda_handler(event, context):
    delete_pending = 0
    # Search for all snapshots
    client = boto3.client('rds', region_name=DEST_REGION)
    response = paginate_api_call(client, 'describe_db_snapshots', 'DBSnapshots')

    # determine array date to retain
    dt = datetime.today()
    retain_day = []

    start_date = dt + timedelta(days=-(32 * RETENTION_MONTHS))
    delta = dt - start_date

    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        if day.weekday() == 0:
            retain_day.append(str(f'{day}T11:'))
    print(retain_day)
    logger.info('Retain date array: %s.' % (str(retain_day)))

    # Filter out the ones not created automatically or with other methods
    filtered_list = get_own_snapshots_dest(PATTERN, response)

    for snapshot in filtered_list.keys():

        creation_date = get_timestamp(snapshot, filtered_list)
        logger.info(creation_date) # 2021-04-01 10:15:00
        if creation_date:
            snapshot_arn = filtered_list[snapshot]['Arn']
            response_tags = client.list_tags_for_resource(
                ResourceName=snapshot_arn)

            if search_tag_copied(response_tags):
                difference = datetime.now() - creation_date
                days_difference = difference.total_seconds() / 3600 / 24

                # check if we need to keep this value for more long
                if not (str(creation_date).replace(" ", "T"))[:14] in retain_day:

                    # if we are past RETENTION_DAYS
                    if days_difference > RETENTION_DAYS:

                        # delete it
                        logger.info('Deleting %s. %s days old' %
                                    (snapshot, days_difference))
                        print('Deleting %s. %s days old' %
                                    (snapshot, days_difference))

                        try:
                            client.delete_db_snapshot(
                                DBSnapshotIdentifier=snapshot)

                        except Exception as e:
                            delete_pending += 1
                            logger.error('Could not delete %s (%s)' % (snapshot, e))

                    else:
                        logger.info('Not deleting %s. Only %s days old' %
                                    (snapshot, days_difference))
                else:
                    logger.info('Not deleting %s. Snapshot timed %s need to be retain for %s month(s)' %
                                (snapshot, creation_date, RETENTION_MONTHS))

            else:
                logger.info(
                    'Not deleting %s. Did not find correct tag' % snapshot)

    if delete_pending > 0:
        log_message = 'Snapshots pending delete: %s' % delete_pending
        logger.error(log_message)
        raise SnapshotToolException(log_message)


if __name__ == '__main__':
    lambda_handler(None, None)
