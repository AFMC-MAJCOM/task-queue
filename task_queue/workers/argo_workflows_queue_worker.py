"""Wherein is contained the class for the Argo Workflow Queue Worker.
"""
import requests
from pprint import pformat

import pandas as pd

from task_queue.workers.queue_worker_interface import QueueWorkerInterface
from task_queue.queues.queue_base import QueueItemStage
from task_queue import logger


class ArgoWorkflowsQueueWorker(QueueWorkerInterface):
    """
    Pushes `queue_item_body.submit_body` directly to the argo workflows rest
    API.

    submit_body schema:
    {
        "namespace": "string",
        "resourceKind": "string",
        "resourceName": "string",
        "submitOptions": {
            "annotations": "string",
            "dryRun": true,
            "entryPoint": "string",
            "generateName": "string",
            "labels": "string",
            "name": "string",
            "ownerReference": {
                "apiVersion": "string",
                "blockOwnerDeletion": true,
                "controller": true,
                "kind": "string",
                "name": "string",
                "uid": "string"
            },
            "parameters": [
                "string"
            ],
            "podPriorityClassName": "string",
            "priority": 0,
            "serverDryRun": true,
            "serviceAccount": "string"
        }
    }
    """

    PAYLOAD_FIELD = "submit_body"
    WORK_QUEUE_ID_LABEL = "work-queue.interface-id"
    WORK_QUEUE_ITEM_ID_LABEL = "work-queue.queue-item-id"


    def __init__(
        self,
        worker_interface_id,
        argo_workflows_endpoint,
        namespace
    ):
        """Initializes ArgoWorkflowQueueInterface

        Parameters:
        -----------
        worker_interface_id: str
            ID of worker interface
        argo_workflows_endpoint: str
            Argo Workflows endpoint
        namespace: str
            Kubernetes namespace for ArgoWorkflowQueueInterface.
        """
        self._worker_interface_id = worker_interface_id
        self._argo_workflows_endpoint = argo_workflows_endpoint
        self._namespace = namespace

    def urlconcat(self, *components):
        """Concatenates URL components into one URL.

        Parameters:
        -----------
        *components: tuple (str)
            Parts of URL.

        Returns:
        -----------
        String of the URL
        """
        return "/".join(c.strip("/") for c in components)

    @property
    def _argo_workflows_submit_url(self):
        """Returns the URL to the argo workflows server which new workflows can
        be submitted to.
        """
        return self.urlconcat(
            self._argo_workflows_endpoint,
            "api",
            "v1",
            "workflows",
            self._namespace,
            "submit"
        )

    @property
    def _argo_workflows_list_url(self):
        """Returns the URL to the argo workflows server that lists the
        workflows.
        """
        return self.urlconcat(
            self._argo_workflows_endpoint,
            "api",
            "v1",
            "workflows",
            self._namespace
        )

    def _argo_workflows_delete_url(self, workflow_name):
        """Returns the URL to the argo workflows server to delete a workflow.
        """
        return self.urlconcat(
            self._argo_workflows_endpoint,
            "api",
            "v1",
            "workflows",
            self._namespace,
            workflow_name
        )

    def _argo_workflows_logs_url(self, workflow_name, log_container):
        """Returns the URL to the argo workflows server to capture a log
        for a specific job.
        """
        return self.urlconcat(
                self._argo_workflows_endpoint,
                "api",
                "v1",
                "workflows",
                self._namespace,
                workflow_name,
                f"log?logOptions.container={log_container}"
        )

    def _construct_submit_body(self, item_id, queue_item_body):
        """Creates body for the submit URL.

        Parameters:
        -----------
        item_id: str
            Queue Item ID.
        queue_item_body: dict
            Dictionary that must contain a key:value pair where the key is
            'submit_body' and the value is a dictionary with the format
            matching the submit_body schema.

        Returns:
        -----------
        Returns a jsonnable dict to send to the submit URL
        """
        payload : dict = queue_item_body[
            ArgoWorkflowsQueueWorker.PAYLOAD_FIELD
            ]
        # Merge new labels into submit options for easier query later.
        # We have to do this get/set because submitOptions and labels
        # may not exist in the queue item body payload
        submit_options = payload.get('submitOptions', {})

        labels = submit_options.get('labels', "")
        labels += (f",{ArgoWorkflowsQueueWorker.WORK_QUEUE_ID_LABEL}"
            f"={self._worker_interface_id}")
        labels += (f",{ArgoWorkflowsQueueWorker.WORK_QUEUE_ITEM_ID_LABEL}"
            f"={item_id}")
        labels = labels.strip(",")

        submit_options['labels'] = labels

        payload['submitOptions'] = submit_options

        return payload

    def _get_workflow_name(self, queue_item_id):
        """Gets the name of the argo workflow using the queue_item_id.

        Parameters:
        -----------
        queue_item_id: str
            Queue Item ID

        Returns:
        -----------
        Returns the name of the argo workflow that corresponds to the given
        Queue Item ID.
        """
        try:
            res = requests.get(self._argo_workflows_list_url, timeout=10)
            res.raise_for_status()

            wf = res.json()

            for item in wf.get("items",[]):
                labels = self.get_labels(item)
                wf_item_id = labels['work-queue.queue-item-id']
                if wf_item_id == queue_item_id:
                    name = item.get("metadata",{}).get("name","Unknown")
                    return name
            return None

        except requests.exceptions.RequestException as e:
            logger.error(e)
            raise e

    def send_job(self, item_id, queue_item_body):
        """Starts a job from queue item.

        Parameters:
        -----------
        item_id: str
            Item ID
        queue_item_body: Dict
            Dictionary that must contain a key:value pair where the key is
            'submit_body' and the value is a dictionary with the format
            matching the submit_body schema.
        """
        request_body = self._construct_submit_body(item_id, queue_item_body)
        request_url = self._argo_workflows_submit_url

        response = requests.post(
            request_url,
            json=request_body,
            timeout=10
        )

        # Check that we got a good response
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            logger.warning("Couldn't submit tasks")
            logger.warning(request_body)
            raise e

    def delete_job(self, queue_item_id):
        """Sends a delete request to argo workflows to delete a specific
        workflow.

        Parameters:
        -----------
        queue_item_id: str
            Item ID of job in workflow
        """
        logs = self.get_logs(queue_item_id)
        for container, log in logs.items():
            logger.info("Item {} container {}: {}".format(queue_item_id, container, pformat(log)))

        name = self._get_workflow_name(queue_item_id)
        delete_url = self._argo_workflows_delete_url(name)
        response = requests.delete(delete_url, timeout = 10)
        try:
            logger.info("Deleting workflow {}".format(name))
            response.raise_for_status()
        except requests.HTTPError as e:
            logger.error("Couldn't delete workflow {}".format(name))
            raise e

    def get_logs(self, queue_item_id):
        """Retrieves the logs of a specific argo workflow.

        Parameters:
        -----------
        queue_item_id: str
            Queue Item ID of the job we are looking for

        Returns:
        ---------
        maybe implemented later
        """
        workflow_name = self._get_workflow_name(queue_item_id)
        log_types = ["main","wait","init"]
        logs = dict()
        for container in log_types:
            log_url = self._argo_workflows_logs_url(workflow_name,container)
            response = requests.get(log_url, timeout=10)
            try: 
                response.raise_for_status()
                logs[container] = response.text
            except requests.HTTPError:
                logger.warning("Couldn't find {} logs for {}".format(container, queue_item_id))
                logs[container] = f"Couldn't find {container} logs for {queue_item_id}"
        return logs

    def _construct_poll_query(self):
        """Creates a dictionary used to ping Argo for information regarding all
        jobs relevant to worker_interface_id.

        Returns:
        -----------
        Dictionary of query.
        """
        # Select labels that match this object
        return {
            "listOptions.labelSelector": (
                f"{ArgoWorkflowsQueueWorker.WORK_QUEUE_ID_LABEL}"
                f"={self._worker_interface_id}"),
            "fields": "items.metadata.labels,metadata.resourceVersion"
        }

    def _get_workflow_status(self, completed, phase):
        """Converts information from the workflow labels into a QueueItemStage.

        workflow status can be read from the labels
        - workflows.argoproj.io/completed='false' -> PROCESSING
        - workflows.argoproj.io/completed='true' and workflows.argoproj.io/
            phase='Succeeded' -> SUCCESS
        - workflows.argoproj.io/completed='true' and not workflows.argoproj.io/
            phase='Succeeded' -> FAIL

        Parameters:
        -----------
        completed: str
            Completion status from the label. ["true", "false"]
        phase: str
            Workflow phase, ["Succeeded", "Failed"]

        Returns:
        -----------
        Status of Item as QueueItemStage object.
        """
        if completed == 'true':
            if phase == "Succeeded":
                return QueueItemStage.SUCCESS

            return QueueItemStage.FAIL

        return QueueItemStage.PROCESSING

    def get_labels(self, wf):
        """Gets workflows metadata labels

        Parameters:
        -----------
        wf: dict
            A workflow item from a list of response body items

        Returns:
        -----------
        Metadata label
        """
        return wf['metadata']['labels']

    def get_workflow_queue_item_id(self, wf):
        """Gets workflow item's ID

        Parameters:
        -----------
        wf: dict
            A workflow item from a list of response body items

        Returns:
        -----------
        Item ID label
        """
        return self.get_labels(wf) \
        [ArgoWorkflowsQueueWorker.WORK_QUEUE_ITEM_ID_LABEL]

    def get_workflow_status(self, wf):
        """Gets the status of the workflow

        Parameters:
        -----------
        wf: dict
            A workflow item from a list of response body items

        Returns:
        -----------
        Workflow status
        """
        argo_completed_label = "workflows.argoproj.io/completed"
        argo_phase_label = "workflows.argoproj.io/phase"

        return self._get_workflow_status(
        self.get_labels(wf).get(argo_completed_label, "false"),
        self.get_labels(wf).get(argo_phase_label, "Pending")
        )

    def get_workflow_create_time(self, wf):
        """Gets the creation timestamp for the workflow from metadata

        Parameters:
        -----------
        wf: dict
            A workflow item from a list of response body items

        Returns:
        -----------
        A timestamp
        """
        return pd.Timestamp(
            wf['metadata']['creationTimestamp'])

    def _get_response_ids_and_status(self, response_body):
        """"Converts the response body of the argo workflows server list
        endpoint into a dictionary of { item_id : queue_item_status }.

        Parameters:
        -----------
        response_body: dict
            Dictionary of the response from Argo Workflow.

        Returns:
        -----------
        Dictionary of the workflow status of each item
        """
        workflows = response_body['items']

        # If there are no workflows to return, `items` will be `null` instead
        # of an empty list.
        if not workflows:
            workflows = []

        # We may still get older workflows with the same worker ID and queue
        # item ID if we have retried an item that has already been run. We can
        # handle this case by only taking the most recent one.

        logger.debug("Filtering results")
        results = {}
        completed_times = {}

        for workflow in workflows:
            timestamp = self.get_workflow_create_time(workflow)
            item_id = self.get_workflow_queue_item_id(workflow)
            if (item_id not in results) or \
                (timestamp > completed_times[item_id]):
                results[item_id] = self.get_workflow_status(workflow)
                completed_times[item_id] = timestamp

        return results


    def poll_all_status(self):
        """Gets the status of each workflow that was submitted by this worker
        from the argo workflows server.

        Returns:
        -----------
        Returns Dict[Any, QueueItemStage]
        """
        logger.debug("Getting status from Argo Workflows")
        request_url = self._argo_workflows_list_url
        request_params = self._construct_poll_query()

        response = requests.get(
            request_url,
            params=request_params,
            timeout=10
        )

        logger.debug("Got response from Argo")

        response.raise_for_status()

        return self._get_response_ids_and_status(response.json())
