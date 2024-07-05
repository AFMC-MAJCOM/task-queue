from enum import Enum

from dataclasses import dataclass
from typing import Callable, Any, List, Tuple, Dict, Optional

class QueueItemStage(Enum):
    WAITING = 0
    PROCESSING = 1
    SUCCESS = 2
    FAIL = 3

@dataclass
class QueueBase():
    put : Callable[[Dict[str, Any]], None]
    get : Callable[[Optional[int]], List[Tuple[str, Any]]]
    success : Callable[[str], Any]
    fail : Callable[[str], Any]
    size : Callable[[QueueItemStage], int]
    lookup_status : Callable[[str], QueueItemStage]
    _description : Dict[str, str]
