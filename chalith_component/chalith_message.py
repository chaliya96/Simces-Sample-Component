from __future__ import annotations
from typing import Any, Dict, Optional

from tools.exceptions.messages import MessageError, MessageValueError
from tools.messages import AbstractResultMessage


class ChalithMessage(AbstractResultMessage):
    """Description for the SimpleMessage class"""
    CLASS_MESSAGE_TYPE = "Chalith"
    MESSAGE_TYPE_CHECK = True

    CHALITH_VALUE_ATTRIBUTE = "ChalithValue"
    CHALITH_VALUE_PROPERTY = "chalith_value"

    # all attributes specific that are added to the AbstractResult should be introduced here
    MESSAGE_ATTRIBUTES = {
        CHALITH_VALUE_ATTRIBUTE: CHALITH_VALUE_PROPERTY
    }
    # list all attributes that are optional here (use the JSON attribute names)
    OPTIONAL_ATTRIBUTES = []

    # all attributes that are using the Quantity block format should be listed here
    QUANTITY_BLOCK_ATTRIBUTES = {}

    # all attributes that are using the Quantity array block format should be listed here
    QUANTITY_ARRAY_BLOCK_ATTRIBUTES = {}

    # all attributes that are using the Time series block format should be listed here
    TIMESERIES_BLOCK_ATTRIBUTES = []

    # always include these definitions to update the full list of attributes to these class variables
    # no need to modify anything here
    MESSAGE_ATTRIBUTES_FULL = {
        **AbstractResultMessage.MESSAGE_ATTRIBUTES_FULL,
        **MESSAGE_ATTRIBUTES
    }
    OPTIONAL_ATTRIBUTES_FULL = AbstractResultMessage.OPTIONAL_ATTRIBUTES_FULL + OPTIONAL_ATTRIBUTES
    QUANTITY_BLOCK_ATTRIBUTES_FULL = {
        **AbstractResultMessage.QUANTITY_BLOCK_ATTRIBUTES_FULL,
        **QUANTITY_BLOCK_ATTRIBUTES
    }
    QUANTITY_ARRAY_BLOCK_ATTRIBUTES_FULL = {
        **AbstractResultMessage.QUANTITY_ARRAY_BLOCK_ATTRIBUTES_FULL,
        **QUANTITY_ARRAY_BLOCK_ATTRIBUTES
    }
    TIMESERIES_BLOCK_ATTRIBUTES_FULL = (
        AbstractResultMessage.TIMESERIES_BLOCK_ATTRIBUTES_FULL +
        TIMESERIES_BLOCK_ATTRIBUTES
    )

    # for each attributes added by this message type provide a property function to get the value of the attribute
    # the name of the properties must correspond to the names given in MESSAGE_ATTRIBUTES
    # template for one property:
    @property
    def chalith_value(self) -> str:
        return self.__simple_value

    @chalith_value.setter
    def chalith_value(self, chalith_value: str):
        self.__simple_value = chalith_value

    def __eq__(self, other: Any) -> bool:
        return (
            super().__eq__(other) and
            isinstance(other, ChalithMessage) and
            self.chalith_value == other.chalith_value
        )

    @classmethod
    def _check_chalith_value(cls, chalith_value: str) -> bool:
        return isinstance(chalith_value, str)

    @classmethod
    def from_json(cls, json_message: Dict[str, Any]) -> Optional[ChalithMessage]:
        """TODO: description for the from_json method"""
        try:
            message_object = cls(**json_message)
            return message_object
        except (TypeError, ValueError, MessageError):
            return None


ChalithMessage.register_to_factory()