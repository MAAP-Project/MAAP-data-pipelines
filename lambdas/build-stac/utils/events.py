import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Literal, Optional, Union

import pystac
from pydantic import BaseModel, Field

INTERVAL = Literal["month", "year"]


class BaseEvent(BaseModel, frozen=True, arbitrary_types_allowed=True):
    collection: str
    remote_fileurl: str

    product_id: Optional[str] = None
    id_regex: Optional[str] = None
    asset_name: Optional[str] = None
    asset_roles: Optional[Union[Dict[str, List[str]], List[str]]] = None
    asset_media_type: Optional[Union[str, dict, pystac.MediaType]] = None
    assets: Optional[Dict[str, str]] = None
    mode: Optional[str] = None
    test_links: Optional[bool] = False
    reverse_coords: Optional[bool]

    def item_id(self: "BaseEvent") -> str:
        if self.id_regex:
            id_components = re.findall(self.id_regex, self.remote_fileurl)
            assert len(id_components) == 1
            return "-".join(id_components[0])
        elif self.product_id:
            return self.product_id
        else:
            return Path(self.remote_fileurl).stem


class CmrEvent(BaseEvent):
    granule_id: str


class RegexEvent(BaseEvent):
    filename_regex: Optional[str]

    start_datetime: Optional[datetime] = None
    end_datetime: Optional[datetime] = None
    single_datetime: Optional[datetime] = None

    properties: Optional[Dict] = Field(default_factory=dict)
    datetime_range: Optional[INTERVAL] = None


SupportedEvent = Union[RegexEvent, CmrEvent]
