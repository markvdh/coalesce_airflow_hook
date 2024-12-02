"""

Airflow Hook for Coalesce

Written by Mark van der Heijden (mark@coalesce.io)

Features:
- Run a Coalesce job
- Keep track of the run status
- Restart a failed run for a configurable amount of times with a configurable interval
- Prevent overloading the Coalesce API
- Uses Snowflake connection defined in Airflow (supports OAuth, KeyPair and basic auth)

Date: Nov 28, 2024

"""

import requests,json,os,sys,logging,time
from datetime import datetime, timedelta
from os import path
from requests.exceptions import HTTPError
from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

def make_request(url, headers, data=None, method="GET", sleep_time=0.0, max_attempts=1, timeout=3600.00):
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
    conn_name_attr = "conn_id"
    default_conn_name = "coalesce_default"
    default_sf_conn = "OAuth"
    conn_type = "http"
    hook_name = "Coalesce"

    def __init__(
        self, conn_id: str = default_conn_name, sf_conn_id: str = default_sf_conn, *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sf_conn_id = sf_conn_id
        self.get_conn()
    
    def get_conn(self):
        try:
            conn = HttpHook.get_connection(self.conn_id)
        except Exception as e:
            raise AirflowException(
                f"Error fetching Coalesce connection details for {self.conn_id}: {e}"
            )
    
        self.host = conn.host or "https://app.coalescesoftware.io"
        self.headers = {
            "Authorization": f"Bearer {conn.password}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        self.GETRUN_ENDPOINT         = self.host + "/api/v1/runs/{runID}"
        self.LISTRUNRESULTS_ENDPOINT = self.host + "/api/v1/runs/{runID}/results"
        self.LISTNODES_ENDPOINT      = self.host + "/api/v1/environments/{envID}/nodes"
        self.GETNODE_ENDPOINT        = self.host + "/api/v1/environments/{envID}/nodes/{nodeID}"
        self.STARTRUN_ENDPOINT       = self.host + "/scheduler/startRun"
        self.RERUN_ENDPOINT          = self.host + "/scheduler/rerun"

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
        return make_request(method="GET", url=self.GETRUN_ENDPOINT.format(runID=run_id), headers=self.headers, sleep_time=0.5, max_attempts=3, timeout=60)

    def apiListRunResults(self, run_id):
        return make_request(method="GET", url=self.LISTRUNRESULTS_ENDPOINT.format(runID=run_id), headers=self.headers, sleep_time=0.5, max_attempts=3, timeout=60)

    def apiListNodes(self, env_id):
        return make_request(method="GET", url=self.LISTNODES_ENDPOINT.format(envID=env_id), headers=self.headers, sleep_time=0.5, max_attempts=3, timeout=60)

    def apiGetNode(self, env_id, node_id):
        return make_request(method="GET", url=self.GETNODE_ENDPOINT.format(envID=env_id, nodeID=node_id), headers=self.headers, sleep_time=0.1, max_attempts=3, timeout=60)

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

        return make_request(method="POST", url=self.STARTRUN_ENDPOINT, headers=self.headers, sleep_time=0.5, data=json.dumps(payload))
    
    def apiRerun(self, run_id):
        payload = {
            "runDetails": {
                "runID": str(run_id)
            },
            "userCredentials": self._get_snowflake_credentials()
        }

        return make_request(method="POST", url=self.RERUN_ENDPOINT, headers=self.headers, sleep_time=0.5, data=json.dumps(payload))
        
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
