from typing import List, Optional
from dataclasses import dataclass


@dataclass
class ValidationConfig:
    ckan_name: str
    validation_name: str
    data_connector_query: dict
    data_asset_name: str
    regex_filter: str
    regex_filter_groups: List[str]
    _suite_name: Optional[str] = None

    @property
    def default_suite_name(self):
        return '@'.join([self.validation_name, self.ckan_name])

    @property
    def suite_name(self):
        if self._suite_name:
            return self._suite_name
        else:
            return self.default_suite_name
