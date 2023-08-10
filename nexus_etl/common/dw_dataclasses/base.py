from dataclasses import dataclass, field
import datetime

@dataclass
class Base:
    _createdat: datetime.datetime=field(
        default=None, 
        metadata={
            "Field Definition":"Metadata field describing when this record was created in the target data store."
        }
    )
    _updatedat: datetime.datetime=field(
        default=None, 
        metadata={
            "Field Definition":"Metadata field describing when this record was updated in the target data store."
        }
    )
    _rev: int=field(
        default=None, 
        metadata={
            "Field Definition":"Metadata field describing how many times this record has been revised. Starting counter is 1."
        }
    )