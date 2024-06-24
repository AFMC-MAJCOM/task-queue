from typing import Generic, TypeVar, Optional, Dict

import pydantic
import datetime

class Event(pydantic.BaseModel):
    name : str
    version : str
    data : pydantic.JsonValue
    id : Optional[int] = None
    event_metadata : Dict[str, pydantic.JsonValue] = pydantic.Field(default_factory=dict)
    time : datetime.datetime = pydantic.Field(default_factory=datetime.datetime.now)
