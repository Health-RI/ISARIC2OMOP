from dataclasses import dataclass
from enum import IntEnum


@dataclass
class Concept:
    concept_id: int
    name: str
    dictionary: str
    domain: str
    concept_class: str
    concept_code: str


class ISARICYesNo(IntEnum):
    yes = 1
    no = 2
    not_available = 3
