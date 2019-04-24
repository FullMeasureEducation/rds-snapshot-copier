// Copyright 2018 Vista Higher Learning, Inc.
// Copyright 2018 Jesse Cotton <jcotton@bitlancer.com>
//
// Licensed under the Apache License, Version 2.0 (the "License"); you
// may not use this file except in compliance with the License.  You
// may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the License for the specific language governing
// permissions and limitations under the License.

'use strict';

const aws = require('aws-sdk');
const dateFormat = require('dateformat');
const slackWebHook = require('@slack/client').IncomingWebhook;

const DR_REGION = process.env.DR_REGION;
const DR_KMS_KEY = process.env.DR_KMS_KEY;

const DATABASE_INSTANCE_FILTER = process.env.DATABASE_INSTANCE_FILTER;

const MAINTAIN_X_SNAPSHOTS = process.env.MAINTAIN_X_SNAPSHOTS;
const SNAPSHOT_COPY_AGE_WARNING = process.env.SNAPSHOT_COPY_AGE_WARNING;
const SNAPSHOT_COPY_AGE_ALERT = process.env.SNAPSHOT_COPY_AGE_ALERT;

const SLACK_WEBHOOK_URL = process.env.SLACK_WEBHOOK_URL;
const SLACK_WARNINGS_CHANNEL = process.env.SLACK_WARNINGS_CHANNEL;
const SLACK_ALERTS_CHANNEL = process.env.SLACK_ALERTS_CHANNEL;

const rdsClient = new aws.RDS();
const rdsClientDr = new aws.RDS({region: DR_REGION});

let formatLogMessage = function(level, msg, dbInstanceId) {
  if(dbInstanceId) {
    return level + ': [' + dbInstanceId + '] ' + msg;
  }
  else {
    return level + ': ' + msg;
  }
};

const logMessage = function(msg, dbInstanceId) {
  console.log(formatLogMessage('INFO', msg, dbInstanceId));
};

const logWarning = function(msg, dbInstanceId) {
  let formattedMsg = formatLogMessage('WARN', msg, dbInstanceId);
  console.log(formattedMsg);
  if(SLACK_WARNINGS_CHANNEL) {
    sendNotification(SLACK_WARNINGS_CHANNEL, formattedMsg);
  }
};

const logError = function(msg, dbInstanceId) {
  let formattedMsg = formatLogMessage('ERR', msg, dbInstanceId);
  console.log(formattedMsg);
  if(SLACK_ALERTS_CHANNEL) {
    sendNotification(SLACK_ALERTS_CHANNEL, formattedMsg);
  }
};

const sendNotification = function(channel, msg) {
  if(!SLACK_WEBHOOK_URL) {
    return;
  }
  const webhook = new slackWebHook(SLACK_WEBHOOK_URL);
  const params = {
    username: 'RDSSnapshotCopier',
    iconEmoji: ':robot_face:',
    channel: channel,
    text: msg
  };
  webhook.send(params, function(err) {
    if(err) {
      console.log(formatLogMessage('ERR', 'Failed to send Slack notification: ' + msg + " to " + channel));
    }
  });
};

const dbClusters = async function() {
  const results = await rdsClient.describeDBClusters({
    MaxRecords: 100
  }).promise();
  return results.DBClusters;
};

const dbClusterSnapshots = async function(clusterIdentifier) {
  const results = await rdsClient.describeDBClusterSnapshots({
    MaxRecords: 100,
    DBClusterIdentifier: clusterIdentifier,
    SnapshotType: 'automated'
  }).promise();
  return results.DBClusterSnapshots;
};

const dbClusterSnapshotDr = async function(snapshotIdentifier) {
  return await rdsClientDr.describeDBClusterSnapshots({
    DBClusterSnapshotIdentifier: snapshotIdentifier
  }).promise();
};

const matchDbInstanceFilter = function(dbInstanceId) {
  if(!DATABASE_INSTANCE_FILTER) return true;
  const dbInstanceFilterRegex = new RegExp(DATABASE_INSTANCE_FILTER);
  return dbInstanceId.match(dbInstanceFilterRegex);
};

const rotateDisasterRecoverySnapshots = async function(dbInstanceId, rotationDate) {
  await copySnapshots(dbInstanceId, rotationDate);
  await deleteOldSnapshots(dbInstanceId, rotationDate);
};

const copySnapshots = async function(clusterIdentifier, rotationDate) {
  const maybeCopySnapshots = [];

  logMessage('Kicking off snapshot copies', clusterIdentifier);
  const clusterSnapshots = await dbClusterSnapshots(clusterIdentifier);
  for (const snapshot of clusterSnapshots) {
    if(snapshot.Status === 'available' && snapshot.SnapshotCreateTime.getTime() > rotationDate.getTime()) {
      maybeCopySnapshots.push(snapshot);
    }
  }
  if(maybeCopySnapshots.length === 0) {
    logMessage('No snapshots need to be copied to the DR region', clusterIdentifier);
    return;
  }

  let snapshotListStr = maybeCopySnapshots.map(function(snapshot) {
    return snapshot.DBClusterSnapshotIdentifier.replace('rds:', '');
  }).join(', ');
  logMessage('Evaluating snapshots for copy: ' + snapshotListStr, clusterIdentifier);

  for (const snapshot of maybeCopySnapshots) {
    let drSnapshotId = snapshot.DBClusterSnapshotIdentifier.replace('rds:', '');
    let nowTimestamp = (new Date().getTime()) / 1000;
    let snapshotTimestamp = (new Date(snapshot.SnapshotCreateTime).getTime()) / 1000;
    let snapshotAgeHours = (nowTimestamp - snapshotTimestamp) / 60 / 60;
    try {
      await dbClusterSnapshotDr(drSnapshotId);
      logMessage("Snapshot " + drSnapshotId + " already exists in DR region.", clusterIdentifier);
    } catch (error) {
      if(error.code === 'DBClusterSnapshotNotFoundFault') {
        let snapshotAgeMsg = "Snapshot " + drSnapshotId + " is " + snapshotAgeHours.toFixed(1) +
            " hours old and has not been copied to the DR region";
        if(snapshotAgeHours > SNAPSHOT_COPY_AGE_ALERT) {
          logError(snapshotAgeMsg, clusterIdentifier);
        }
        else if(snapshotAgeHours > SNAPSHOT_COPY_AGE_WARNING) {
          logWarning(snapshotAgeMsg, clusterIdentifier);
        }
        const params = {
          SourceDBClusterSnapshotIdentifier: snapshot.DBClusterSnapshotArn,
          TargetDBClusterSnapshotIdentifier: drSnapshotId,
          CopyTags: true,
          SourceRegion: snapshot.DBClusterSnapshotArn.split(":")[3]
        };
        if(snapshot.StorageEncrypted) {
          params.KmsKeyId = DR_KMS_KEY;
        }
        logMessage('Copying snapshot ' + drSnapshotId + ' to DR region', clusterIdentifier);
        try {
          await rdsClientDr.copyDBClusterSnapshot(params).promise();
        } catch (error) {
          if(error.code === 'SnapshotQuotaExceeded') {
            logWarning('Ignoring snapshot copy quota error: ' + JSON.stringify(error), clusterIdentifier);
          } else {
            throw error;
          }
        }
      } else {
        throw error;
      }
    }
  }
};

const deleteOldSnapshots = async function(clusterIdentifier, rotationDate) {
  const oldestAllowedSnapshot = clusterIdentifier + '-' + dateFormat(rotationDate, 'yyyy-mm-dd-HH-MM');
  const oldSnapshots = [];

  logMessage('Kicking off snapshot deletion in DR region', clusterIdentifier);
  logMessage('Finding snapshots older than ' + oldestAllowedSnapshot, clusterIdentifier);

  const destinationSnapshotsResponse = await rdsClientDr.describeDBClusterSnapshots({
    DBClusterIdentifier: clusterIdentifier,
    MaxRecords: 100,
    SnapshotType: 'manual'
  }).promise();
  for (const snapshot of destinationSnapshotsResponse.DBClusterSnapshots) {
    if(snapshot.DBClusterSnapshotIdentifier < oldestAllowedSnapshot) {
      oldSnapshots.push(snapshot.DBClusterSnapshotIdentifier);
    }
  }
  if(oldSnapshots.length === 0) {
    logMessage('No snapshots marked for deletion', clusterIdentifier);
  } else {
    for (const oldSnapshotId of oldSnapshots) {
      logMessage('Deleting snapshot ' + oldSnapshotId, clusterIdentifier);
      await rdsClientDr.deleteDBClusterSnapshot({DBClusterSnapshotIdentifier: oldSnapshotId}).promise();
    }
  }
};

exports.handler = async function(event) {
  logMessage('Received event: ' + JSON.stringify(event));

  let rotationDate = new Date();
  rotationDate.setDate(rotationDate.getDate() - MAINTAIN_X_SNAPSHOTS);

  try {
    logMessage('Kicking off snapshot rotation for all instances');
    const clusters = await dbClusters();
    for (let cluster of clusters) {
      let clusterIdentifier = cluster.DBClusterIdentifier;
      if(cluster.BackupRetentionPeriod !== 0) {
        if(matchDbInstanceFilter(clusterIdentifier)) {
          await rotateDisasterRecoverySnapshots(clusterIdentifier, rotationDate);
        } else {
          logMessage("Skipping database b/c it does not match filter", clusterIdentifier);
        }
      } else {
        logMessage('Skipping snapshot rotation b/c backups are disabled', clusterIdentifier);
      }
    }
  } catch(err) {
    logError('Fatal error: ' + JSON.stringify(err));
  }
};
