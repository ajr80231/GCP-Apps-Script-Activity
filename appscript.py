#!/usr/bin/env python
#
# Copyright 2018 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Simple tool to retrieve Data regarding App Script Projects associated with a
Google Cloud Organization. The assumption is that all these projects 
are found under: Ancestry: Organization > system-gsuite > apps-script
"""

import logging
import warnings
import click
import json

import concurrent.futures
from google.api_core.exceptions import GoogleAPIError
from datetime import datetime, timedelta
import googleapiclient.discovery
import googleapiclient.errors

_LOGGER = logging.getLogger('apps-script-analytics')
_ACTIVITY = []


class Error(Exception):
    pass


def _configure_logging(verbose=True, stackdriver_logging=False):
    """
    Basic Logging Configuration
    Args:
        verbose: enable verbose logging
        stackdriver_logging:  enable stackdriver logging

    """
    logging.basicConfig(level=logging.ERROR)
    level = logging.DEBUG if verbose else logging.INFO
    logging.getLogger(_LOGGER.name).setLevel(level)
    warnings.filterwarnings('ignore', r'.*end user credentials.*', UserWarning)
    if stackdriver_logging:
        import google.cloud.logging
        client = google.cloud.logging.Client()
        client.setup_logging(log_level=level)


def _validate_folder_id(folder_id, client=None):
    """
    Validate that the given folder_id is the folder in relationship to:
    Ancestry: organization > system-gsuite > apps-script [folder_id]
    Args:
        folder_id: Folder Created by GCP to host App Script Projects
        client: optional instance of googleapiclient.discovery.build will be used
            instead of obtaining a new one

    """
    try:
        _LOGGER.debug('Verifying if %s is the App Scripts Folder of the Organization', folder_id)
        client = client or googleapiclient.discovery.build('cloudresourcemanager', 'v2', cache_discovery=False)
        req = client.folders().get(name="folders/{}".format(folder_id))
        resp = req.execute()
        _LOGGER.debug(resp)
        if resp['displayName'] != 'apps-script':
            raise Error('Error this is not the apps-script folder')
    except (GoogleAPIError, googleapiclient.errors.HttpError, Exception) as e:
        _LOGGER.info('API Error: %s', e, exc_info=True)
        raise Error("Error Validating folderId: %s", folder_id)


def _fetch_projects(folder_id, client=None):
    """
    Return all App Script Projects tied to the organization
    Args:
        folder_id: Ancestry: organization > system-gsuite > apps-script
        client: optional instance of googleapiclient.discovery.build will be used
            instead of obtaining a new one

    Returns: A dict response

    """
    try:
        client = client or googleapiclient.discovery.build('cloudresourcemanager', 'v1', cache_discovery=False)
        projects = []
        req = client.projects().list(filter="parent.id={}".format(folder_id))
        resp = req.execute()
        projects.extend(resp["projects"])
        page_token = resp.get('nextPageToken', None)
        while page_token is not None:
            _LOGGER.debug("Fetching Next Page of Results", page_token)
            req = client.projects().list_next(previous_request=req, previous_response=resp)
            resp = req.execute()
            projects.extend(resp["projects"])

        _LOGGER.debug("Found {} App Script Projects".format(len(projects)))
        projects.sort(key=lambda x: datetime.strptime(x['createTime'], '%Y-%m-%dT%H:%M:%S.%fZ'))

        return {"projects": projects, "total": len(projects)}
    except (GoogleAPIError, googleapiclient.errors.HttpError, Exception) as e:
        _LOGGER.info('API Error: %s', e, exc_info=True)
        raise Error("Error fetching projects under folderId: %s", folder_id)


def __build_fetch_projects_latest_activity_request(projects, freshness=15):
    freshness = datetime.today() - timedelta(days=freshness)
    input_data = {

        "filter": "timestamp>=\"{}\" AND resource.type=app_script_function".format(
            freshness.strftime('%Y-%m-%dT%H:%M:%S.%fZ')),
        "pageSize": 1,  # Ensures we get the latest log entry
    }
    requests = []
    for project in projects['projects']:
        input_data['resourceNames'] = "projects/{}".format(project['projectId'])
        requests.append(input_data.copy())
    _LOGGER.debug("Retrieving Recent Activity for {} projects".format(len(requests)))
    return requests


def __fetch_projects_latest_activity_worker(request):
    try:
        client = googleapiclient.discovery.build('logging', 'v2', cache_discovery=False)
        req = client.entries().list(body=request)
        resp = req.execute()
        _ACTIVITY.append(resp)
        _LOGGER.debug("Appended {} results".format(len(_ACTIVITY)))
    except (GoogleAPIError, googleapiclient.errors.HttpError) as e:
        _LOGGER.debug('API Error: %s', e, exc_info=True)
        raise Error("Error Retrieve Project Recent Activity")


def _fetch_projects_latest_activity(projects, client=None, max_workers=3):
    """
    Leverage Cloud Logging to retrieve last execution time of an App Script.
    Cloud logging logs this under resource.type=app_script_function
    Args:
        projects: List of Projects to retrieve App Script Activity Usage
        client: optional instance of googleapiclient.discovery.build will be used
            instead of obtaining a new one

    Returns: A dict response

    """
    try:
        requests = __build_fetch_projects_latest_activity_request(projects)
        with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
            executor.map(__fetch_projects_latest_activity_worker, requests)
        _ACTIVITY[:] = [{"project_id": x['entries'][0]["resource"]["labels"]["project_id"],
                         "timestamp": x['entries'][0]["timestamp"],
                         "script_info": x['entries'][0]["labels"]} for x in _ACTIVITY]
        _ACTIVITY.sort(key=lambda x: datetime.strptime(x['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ'))

        return _ACTIVITY

    except (GoogleAPIError, googleapiclient.errors.HttpError) as e:
        _LOGGER.info('API Error: %s', e, exc_info=True)
        raise Error("Error Retrieve Project Recent Activity")


@click.group(invoke_without_command=False)
@click.option('--folder-id', required=True, help='Folder Id: Organization > system-gsuite > apps-script')
@click.option('--stackdriver-logging', is_flag=True, default=False,
              help='Send logs to Stackdriver')
@click.option('--verbose', is_flag=True, default=False, help='Verbose output')
@click.pass_context
def main(ctx, folder_id=None, key_file=None, verbose=False, stackdriver_logging=False, **kwargs):
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    _configure_logging(verbose=verbose, stackdriver_logging=stackdriver_logging)
    ctx.ensure_object(dict)  # type dictionary
    ctx.obj = locals()


@main.command(name="list")
@click.pass_context
def retrieve(ctx):
    """
    List all App Script projects with creation time.
    Args:
        ctx: Click Object containing parameters passed

    Returns: App Script JSON information

    """
    try:
        _validate_folder_id(ctx.obj['folder_id'])
        resp = _fetch_projects(ctx.obj['folder_id'])
        logging.debug(resp)
        click.echo(json.dumps(resp, indent=4, sort_keys=True))
    except Error as e:
        _LOGGER.critical(e)


@main.command()
@click.pass_context
def activity(ctx):
    """
    Fetches Activity information for all the projects found under
    Ancestry: organization > system-gsuite > apps-script
    Args:
        ctx: Object of type click

    Returns: All the projects found under Ancestry: organization > system-gsuite > apps-script last invocation time

    """
    try:
        _validate_folder_id(ctx.obj['folder_id'])
        projects = _fetch_projects(ctx.obj['folder_id'])
        _LOGGER.debug("Fetched All GCP App Script Projects")
        resp = _fetch_projects_latest_activity(projects)
        click.echo(json.dumps(resp, indent=4, sort_keys=True))
    except Error as e:
        _LOGGER.critical(e)


if __name__ == '__main__':
    main(auto_envvar_prefix='OPT')
