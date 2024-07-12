import requests

import pandas as pd

from data_pipeline.queue_worker_interface import QueueWorkerInterface
from data_pipeline.queue_base import QueueItemStage


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
        worker_interface_id:str,
        argo_workflows_endpoint:str,
        namespace:str
    ):
        self._worker_interface_id = worker_interface_id
        self._argo_workflows_endpoint = argo_workflows_endpoint
        self._namespace = namespace

    def urlconcat(*components):
        return "/".join(c.strip("/") for c in components)

    @property
    def _argo_workflows_submit_url(self):
        return ArgoWorkflowsQueueWorker.urlconcat(
            self._argo_workflows_endpoint,
            "api",
            "v1",
            "workflows",
            self._namespace,
            "submit"
        )

    @property
    def _argo_workflows_list_url(self):
        return ArgoWorkflowsQueueWorker.urlconcat(
            self._argo_workflows_endpoint,
            "api",
            "v1",
            "workflows",
            self._namespace
        )


    def _construct_submit_body(self, item_id, queue_item_body):
        """
        Returns a jsonnable dict to send to the submit URL
        """

        payload : dict = queue_item_body[
            ArgoWorkflowsQueueWorker.PAYLOAD_FIELD
            ]

        # merge new labels into submit options for easier query later
        # we have to do this get/set stuff because submitOptions and labels
        # may not existin the queue item body payload
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

    def send_job(self, item_id, queue_item_body):
        request_body = self._construct_submit_body(item_id, queue_item_body)
        request_url = self._argo_workflows_submit_url

        response = requests.post(
            request_url,
            json=request_body
        )

        # check that we got a good response
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            print("Couldn't submit tasks")
            print(request_body)
            raise e

    def _construct_poll_query(self):
        # select labels that match this object
        return {
            "listOptions.labelSelector": (
                f"{ArgoWorkflowsQueueWorker.WORK_QUEUE_ID_LABEL}"
                f"={self._worker_interface_id}"),
            "fields": "items.metadata.labels,metadata.resourceVersion"
        }

    def _get_workflow_status(self, completed, phase):
        """
        workflow status can be read from the labels
        - workflows.argoproj.io/completed='false' -> PROCESSING
        - workflows.argoproj.io/completed='true' and workflows.argoproj.io/
            phase='Succeeded' -> SUCCESS
        - workflows.argoproj.io/completed='true' and not workflows.argoproj.io/
            phase='Succeeded' -> FAIL
        """

        if completed == 'true':
            if phase == "Succeeded":
                return QueueItemStage.SUCCESS
            else:
                return QueueItemStage.FAIL
        else:
            return QueueItemStage.PROCESSING


    def _get_response_ids_and_status(self, response_body):
        workflows = response_body['items']

        # If there are no workflows to return, `items` will be `null` instead
        # of an empty list.
        if not workflows:
            workflows = []

        argo_completed_label = "workflows.argoproj.io/completed"
        argo_phase_label = "workflows.argoproj.io/phase"

        # queue item ID and status are labels in `metadata.labels`
        labels = lambda wf: wf['metadata']['labels']
        get_workflow_queue_item_id = lambda wf: labels(wf) \
            [ArgoWorkflowsQueueWorker.WORK_QUEUE_ITEM_ID_LABEL]
        get_workflow_status = lambda wf: self._get_workflow_status(
            labels(wf).get(argo_completed_label, "false"),
            labels(wf).get(argo_phase_label, "Pending")
        )
        get_workflow_create_time = lambda wf: pd.Timestamp(
            wf['metadata']['creationTimestamp'])

        # We may still get older workflows with the same worker ID and queue
        # item ID if we have retried an item that has already been run. We can
        # handle this case by only taking the most recent one.

        print("Filtering results")
        results = {}
        completed_times = {}

        for workflow in workflows:
            timestamp = get_workflow_create_time(workflow)
            item_id = get_workflow_queue_item_id(workflow)
            if (item_id not in results) or \
                (timestamp > completed_times[item_id]):
                results[item_id] = get_workflow_status(workflow)
                completed_times[item_id] = timestamp

        return results


    def poll_all_status(self):
        print("Getting status from Argo Workflows")
        request_url = self._argo_workflows_list_url
        request_params = self._construct_poll_query()

        response = requests.get(
            request_url,
            params=request_params,
        )

        print("Got response from Argo")

        response.raise_for_status()

        return self._get_response_ids_and_status(response.json())
