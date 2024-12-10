"""Releasess jobs if resources are available.
"""

from .job_release_strategy_base import JobReleaseStrategyBase
from ..queues.queue_base import QueueItemStage

def sum_dictionaries(dictionaries):
    """
    Sum the values of each dictionary in an iterable, filling in zeros for
    values that don't exist.
    """
    out_dict = {}
    for dict in dictionaries:
        for k, v in dict.items():
            if not k in out_dict:
                out_dict[k] = 0
            out_dict[k] += v

    return out_dict

def all_values_negative(dict_of_numbers):
    return all(r < 0 for r in dict_of_numbers)

class ResourceLimit(JobReleaseStrategyBase):
    """
    Releases new jobs while there are resources available for the next queue
    items.
    """

    def __init__(
            self,
            resource_limits:dict[str, int],
            resource_key:str="resources"
        ):
        self.resource_key = resource_key
        self.resource_limits = resource_limits.copy()

        # So we can add a negative number to do a subtraction later.
        self.negative_resource_limits = { k:-v for k,v in self.resource_limits.items() }


    def release_next_jobs(self, work_queue):
        jobs_processing = work_queue._queue.lookup_state(QueueItemStage.PROCESSING)
        resources_used_by_processing = sum_dictionaries(
            (
                work_queue._queue
                .lookup_item(item_id)["item_body"]
                .get(self.resource_key, {})
            )
            for item_id in jobs_processing
        )
        negative_available_resources = sum_dictionaries(
            [
                resources_used_by_processing,
                self.negative_resource_limits
            ]
        )

        assert all_values_negative(negative_available_resources)

        done = False
        while not done:
            next_possible_items = work_queue._queue.peek(10)

            jobs_to_push = 0
            for _, item_body in next_possible_items:
                # calculate what the available resources would be if we
                # started this job
                negative_available_resources = sum_dictionaries(
                    [
                        negative_available_resources,
                        item_body[self.resource_key]
                    ]
                )

                # check if we have the resources to start this job.
                if not all_values_negative(negative_available_resources):
                    done = True
                    break
                else:
                    jobs_to_push += 1

            # start the next batch of jobs. When jobs are pushed, they are
            # moved from WAITING to PROCESSING, so they will not be returned
            # by `queue.peek` in the next loop.
            work_queue.push_next_jobs(jobs_to_push)


    def can_fit_jobs(self, new_job_resources):
        current_resources_used = sum_dictionaries(self.processing_job_resources.values())

        # Subtract the resource limits from the resources required by the
        # current processing jobs plus the new job. If any outputs are
        # positive, that means we would be starting jobs that require more
        # resources than the limit allows.
        new_resources_remaining = sum_dictionaries(
            [
                current_resources_used,
                new_job_resources,
                self.negative_resource_limits
            ]
        )

        return all( r < 0 for r in new_resources_remaining.values() )


