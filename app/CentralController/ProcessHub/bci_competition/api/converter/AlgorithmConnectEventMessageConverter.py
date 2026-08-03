from typing import Union

from google.protobuf.message import Message

from ProcessHub.bci_competition.api.model.AlgorithmConnectEventModel import AlgorithmConnectClosedEventModel, \
    AlgorithmConnectEventModel
from ProcessHub.bci_competition.api.protobuf.AlgorithmConnectEvent_pb2 import (
    AlgorithmConnectClosedEventMessage as AlgorithmConnectClosedEventMessage_pb2,
    AlgorithmConnectEventMessage as AlgorithmConnectEventMessage_pb2,
)


def ensure_initialization(cls):
    if not hasattr(cls, '_has_been_initialized'):
        cls.initial()
        setattr(cls, '_has_been_initialized', True)
    return cls


@ensure_initialization
class AlgorithmConnectEventMessageConverter:
    __package_name_for_convert_func_dict: dict
    __model_class_for_convert_func_dict: dict

    @classmethod
    def initial(cls):
        cls.__package_name_for_convert_func_dict = {
            AlgorithmConnectClosedEventMessage_pb2: cls.__algorithm_connect_closed_event_message_to_model,
            AlgorithmConnectEventMessage_pb2: cls.__algorithm_connect_event_message_to_model,
        }
        cls.__model_class_for_convert_func_dict = {
            AlgorithmConnectClosedEventModel: cls.__algorithm_connect_closed_event_model_to_message_pb,
            AlgorithmConnectEventModel: cls.__algorithm_connect_event_model_to_message_pb,
        }

    @classmethod
    def protobuf_to_model(cls, pb_message: Message) -> Union[
        AlgorithmConnectClosedEventModel,
        AlgorithmConnectEventModel,
    ]:
        convert_func = cls.__package_name_for_convert_func_dict[type(pb_message)]
        return convert_func(pb_message)

    @classmethod
    def model_to_protobuf(cls, model: Union[
        AlgorithmConnectClosedEventModel,
        AlgorithmConnectEventModel,
    ]
                          ) -> Message:
        convert_func = cls.__model_class_for_convert_func_dict[type(model)]
        return convert_func(model)

    @classmethod
    def __algorithm_connect_closed_event_message_to_model(
            cls, algorithm_connect_closed_event_message: AlgorithmConnectClosedEventMessage_pb2) -> AlgorithmConnectClosedEventModel:
        return AlgorithmConnectClosedEventModel(
            address=algorithm_connect_closed_event_message.address
        )

    @classmethod
    def __algorithm_connect_event_message_to_model(
            cls, algorithm_connect_event_message: AlgorithmConnectEventMessage_pb2) -> AlgorithmConnectEventModel:
        package_name = algorithm_connect_event_message.WhichOneof('package')
        return AlgorithmConnectEventModel(
            package=cls.__algorithm_connect_closed_event_message_to_model(
                algorithm_connect_event_message.algorithmConnectClosedEventMessage)
            if package_name == "algorithmConnectClosedEventMessage"
            else None
        )

    @classmethod
    def __algorithm_connect_closed_event_model_to_message_pb(cls, algorithm_connect_closed_event_model: AlgorithmConnectClosedEventModel) \
            -> AlgorithmConnectClosedEventMessage_pb2:
        return AlgorithmConnectClosedEventMessage_pb2(
            address=algorithm_connect_closed_event_model.address
        )

    @classmethod
    def __algorithm_connect_event_model_to_message_pb(cls, algorithm_connect_event_model: AlgorithmConnectEventModel) \
            -> AlgorithmConnectEventMessage_pb2:
        package = algorithm_connect_event_model.package
        if isinstance(package, AlgorithmConnectClosedEventModel):
            return AlgorithmConnectEventMessage_pb2(
                algorithmConnectClosedEventMessage=cls.__algorithm_connect_closed_event_model_to_message_pb(
                    algorithm_connect_event_model.package)
            )
