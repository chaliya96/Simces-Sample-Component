# Author(s): Chalith Tharuka <chalith.haputhantrige@tuni.fi>

import asyncio
import traceback
from typing import Any, cast, Set, Optional, Union

from tools.components import AbstractSimulationComponent
from tools.exceptions.messages import MessageError
from tools.messages import BaseMessage
from tools.tools import FullLogger, load_environmental_variables, log_exception

from chalith_component.chalith_message import ChalithMessage

LOGGER = FullLogger(__name__)

CHALITH_VALUE = "CHALITH_VALUE"
INPUT_COMPONENTS = "INPUT_COMPONENTS"

CHALITH_TOPIC = "CHALITH_TOPIC"
SIMPLE_TOPIC = "SIMPLE_TOPIC"

TIMEOUT = 1.0

class ChalithComponent(AbstractSimulationComponent):


    def __init__(self,input_components: Set[str],chalith_value: str):
        
        super().__init__()

        self.chalith_value = chalith_value
        self._input_components = input_components


        self._current_message = ""
        self._current_input_components = set()
    
        environment = load_environmental_variables(
            (CHALITH_TOPIC, str, "ChalithTopic")
        )
        self._chalith_topic_base = cast(str, environment[CHALITH_TOPIC])
        self._chalith_topic_output = ".".join([self._chalith_topic_base, self.component_name])

        # The easiest way to ensure that the component will listen to all necessary topics
        # is to set the self._other_topics variable with the list of the topics to listen to.
        # Note, that the "SimState" and "Epoch" topic listeners are added automatically by the parent class.
        self._other_topics = [
            ".".join([self._chalith_topic_base, other_component_name])
            for other_component_name in self._input_components
        ]


    def clear_epoch_variables(self) -> None:
        self._current_message = ""
        self._current_input_components = set()

    async def process_epoch(self) -> bool:
        if self._current_input_components:
            self._current_message = self._current_message + self.chalith_value
        else:
            self._current_message = self.chalith_value
        
        await self._send_chalith_message()
        return True

    
    async def _send_chalith_message(self):
        """
        Sends a SimpleMessage using the self._current_number_sum as the value for attribute SimpleValue.
        """
        try:
            chalith_message = self._message_generator.get_message(
                ChalithMessage,
                EpochNumber=self._latest_epoch,
                TriggeringMessageIds=self._triggering_message_ids,
                ChalithValue=self._current_message
            )

            await self._rabbitmq_client.send_message(
                topic_name=self._chalith_topic_output,
                message_bytes=chalith_message.bytes()
            )

        except (ValueError, TypeError, MessageError) as message_error:
            # When there is an exception while creating the message, it is in most cases a serious error.
            log_exception(message_error)
            await self.send_error_message("Internal error when creating result message.")

    async def all_messages_received_for_epoch(self) -> bool:
        return self._input_components == self._current_input_components

    async def general_message_handler(self, message_object: Union[BaseMessage, Any],
                                      message_routing_key: str) -> None:
                                
        if isinstance(message_object, ChalithMessage):
            message_object = cast(ChalithMessage, message_object)
            if message_object.source_process_id not in self._input_components:
                LOGGER.debug(f"Ignoring SimpleMessage from {message_object.source_process_id}")

            elif message_object.source_process_id in self._current_input_components:
                LOGGER.info(f"Ignoring new SimpleMessage from {message_object.source_process_id}")

            else:
                self._current_input_components.add(message_object.source_process_id)
                self._current_message = self._current_message + message_object.chalith_value
                LOGGER.debug(f"Received SimpleMessage from {message_object.source_process_id}")

                self._triggering_message_ids.append(message_object.message_id)
                if not await self.start_epoch():
                    LOGGER.debug(f"Waiting for other input messages before processing epoch {self._latest_epoch}")

        else:
            LOGGER.debug("Received unknown message from {message_routing_key}: {message_object}")


def create_component() -> ChalithComponent:

    LOGGER.debug("create")
    environment_variables = load_environmental_variables(
        (CHALITH_VALUE, str, "test"),  
        (INPUT_COMPONENTS, str, "")  
    )
    chalith_value = cast(str, environment_variables[CHALITH_VALUE])
    input_components = {
        input_component
        for input_component in cast(str, environment_variables[INPUT_COMPONENTS]).split(",")
        if input_component
    }

    return ChalithComponent(
        chalith_value=chalith_value,
        input_components=input_components,
    )


async def start_component():
    try:
        LOGGER.debug("start")
        chalith_component = create_component()
        await chalith_component.start()

        while not chalith_component.is_stopped:
            await asyncio.sleep(TIMEOUT)

    except BaseException as error:  # pylint: disable=broad-except
        log_exception(error)
        LOGGER.info("Component will now exit.")




if __name__ == "__main__":
    asyncio.run(start_component())
