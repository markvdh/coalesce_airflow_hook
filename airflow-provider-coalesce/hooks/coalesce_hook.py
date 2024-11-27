"""

Airflow Hook for Coalesce

Written by Mark van der Heijden (mark@coalesce.io)

Features:
- Run a Coalesce job
- Keep track of the run status
- Restart a failed run for a configurable amount of times with a configurable interval
- Prevent overloading the Coalesce API
- Uses Snowflake connection defined in Airflow (supports OAuth, KeyPair and basic auth)

Date: Nov 26, 2024

"""

import requests,json,os,sys,logging,time
from datetime import datetime, timedelta
from os import path
from requests.exceptions import HTTPError
from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException

# Start configuration

# Base URL - change this to include specific clusters, subdomain, etc.
BASE_URL = "https://app.coalescesoftware.io"

# API endpoint URL templates - usually don't need changing
GETRUN_ENDPOINT         = BASE_URL + "/api/v1/runs/{runID}"
LISTRUNRESULTS_ENDPOINT = BASE_URL + "/api/v1/runs/{runID}/results"
LISTNODES_ENDPOINT      = BASE_URL + "/api/v1/environments/{envID}/nodes"
GETNODE_ENDPOINT        = BASE_URL + "/api/v1/environments/{envID}/nodes/{nodeID}"
STARTRUN_ENDPOINT       = BASE_URL + "/scheduler/startRun"
RERUN_ENDPOINT          = BASE_URL + "/scheduler/rerun"

# End configuration

logger = logging.getLogger(__name__)

def make_request(url, headers, data=None, method="GET", sleep_time=0.0, max_attempts=1, timeout=3600.00):
    """
    Make requests to any API with an automatic retry and rate limit mechanism.

    Raises:
        HTTPError Exception: If response status code!= 200.

    Returns:
        Response: The response from the API call.
    """
    # API is rate-limited, adjust the sleep time based on the rate limit of the API you are calling
    time.sleep(sleep_time)

    if method not in ["POST", "PATCH", "PUT", "GET", "DELETE"]:
        raise ValueError("Method not supported. Available methods are: POST, PATCH, PUT, GET & DELETE")

    kwargs = {"url": url, "headers": headers, "data": data, "timeout": timeout}

    request_methods = {
        'POST': requests.post,
        'PATCH': requests.patch,
        'PUT': requests.put,
        'DELETE': requests.delete,
        'GET': requests.get
    }

    for attempt in range(max_attempts):
        try:
            response = request_methods[method](**kwargs)
            return response.json()
        except HTTPError as e:
            if attempt < max_attempts - 1:
                logger.warning(f"Retrying, Request failed, Error: {e}")
            else:
                raise e

class coalesce_hook(HttpHook):
    """
    Make Reliable API Calls to Coalesce
    :param conn_id: ID of your airflow connection, containing
        Coalesce token in the 'password' field, and 'https' in the 'schema' field
    :type conn_id: str
    """

    def __init__(self, conn_id=None, sf_conn_id="OAuth", *args, **kwargs):
        self.conn_id = conn_id
        self.sf_conn_id = sf_conn_id
        self.headers = {
            "Authorization": f"Bearer {self._get_coalesce_token()}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def _get_coalesce_token(self):
        """
        Get Coalesce token from connection id.
        Raises:
            AirflowException: If there is no valid token or conn_id supplied.
        Returns:
            str: The Coalesce token.
        """
        try:
            return HttpHook.get_connection(self.conn_id).password
        except Exception as e:
            raise AirflowException(
                f"Error fetching connection details for {self.conn_id}: {e}"
            )
    
    def _get_snowflake_credentials(self):
        # Initialize an empty dictionary to hold the credentials
        credentials = {}
        # OAuth if neither username nor private key is provided

        if self.sf_conn_id == 'OAuth':
            snowflakeAuthType = 'OAuth'
            
        else:
            try:
                conn = HttpHook.get_connection(self.sf_conn_id)

                # Determine the snowflakeAuthType based on the connection fields
                if conn.login and conn.password and not conn.extra_dejson.get('private_key_content', None):
                    # Basic authentication if both username and password are provided, and no private key
                    snowflakeAuthType = 'Basic'
                    credentials['snowflakeUsername'] = conn.login
                    credentials['snowflakePassword'] = conn.password
                
                elif conn.extra_dejson.get('private_key_content', None):
                    # KeyPair authentication if a private key is provided
                    snowflakeAuthType = 'KeyPair'
                    credentials['snowflakeKeyPairKey'] = conn.extra_dejson.get('private_key_content', None)  # Get private key from extra
                    if conn.password:  # If there's a password, include it as snowflakeKeyPairPass
                        credentials['snowflakeKeyPairPass'] = conn.password

                # Always pass the warehouse and role if they are provided
                if conn.extra_dejson.get('warehouse', None):
                    credentials['snowflakeWarehouse'] = conn.extra_dejson['warehouse']
                
                if conn.extra_dejson.get('role', None):
                    credentials['snowflakeRole'] = conn.extra_dejson['role']

            except Exception as e:
                raise AirflowException(
                    f"Error fetching connection details for {self.sf_conn_id}: {e}"
                )

        credentials['snowflakeAuthType'] = snowflakeAuthType

        # Return the final credentials dictionary
        return credentials

    def apiGetRun(self, run_id):
        return make_request(method="GET", url=GETRUN_ENDPOINT.format(runID=run_id), headers=self.headers, sleep_time=0.5, max_attempts=3, timeout=60)

    def apiListRunResults(self, run_id):
        return make_request(method="GET", url=LISTRUNRESULTS_ENDPOINT.format(runID=run_id), headers=self.headers, sleep_time=0.5, max_attempts=3, timeout=60)

    def apiListNodes(self, env_id):
        return make_request(method="GET", url=LISTNODES_ENDPOINT.format(envID=env_id), headers=self.headers, sleep_time=0.5, max_attempts=3, timeout=60)

    def apiGetNode(self, env_id, node_id):
        return make_request(method="GET", url=GETNODE_ENDPOINT.format(envID=env_id, nodeID=node_id), headers=self.headers, sleep_time=0.1, max_attempts=3, timeout=60)

    def apiStartRun(self, env_id, job_id=None):
        payload = {
            "runDetails": {
                "parallelism": 16,
                "environmentID": str(env_id),
            },
            "userCredentials": self._get_snowflake_credentials()
        }
        if job_id:
            payload["runDetails"]["jobID"] = str(job_id)

        logger.info(f"Using {payload['userCredentials']['snowflakeAuthType']} authentication for Snowflake")

        return make_request(method="POST", url=STARTRUN_ENDPOINT, headers=self.headers, sleep_time=0.5, data=json.dumps(payload))
    
    def apiRerun(self, run_id):
        payload = {
            "runDetails": {
                "runID": str(run_id)
            },
            "userCredentials": self._get_snowflake_credentials()
        }

        return make_request(method="POST", url=RERUN_ENDPOINT, headers=self.headers, sleep_time=0.5, data=json.dumps(payload))
        
    def startRun(self, env_id, job_id, offline_mode = False, retries=3, delay=30):
        runStart = self.apiStartRun(env_id, job_id)
        try:
            runId = runStart["runCounter"]
        except KeyError:
            raise AirflowException(f"Failed starting job: {runStart['error']['errorString']}")
        rerunId = None
        logger.info(f"Job started, run id = {runId}")
        runStatus = "running"
        retry = 0
        s = "s" if delay > 1 else ""
        time.sleep(10)

        while runStatus in ('rendering','waitingToRun','running'):
            id = rerunId if rerunId else runId
            runGet = self.apiGetRun(id)
            runStatus = runGet["runStatus"]

            logger.info(f"Job run with id {id} is {runStatus}")

            if runStatus == 'failed':
                if retry < retries:
                    retry += 1
                    logger.info(f"Waiting for {delay} second{s} to restart failed run")
                    time.sleep(delay)
                    logger.info(f"Restarting failed run (attempt {retry} of {retries})")
                    runRerun = self.apiRerun(id)
                    rerunId = runRerun["runCounter"]
                    logger.info(f"Rerun started, rerun id = {rerunId}")
                    runStatus = "running"
                    time.sleep(10)
                else:
                    raise AirflowException(f"Job run with id {id} failed and out of retries")

            else:
                time.sleep(10)
