""" Central mediator of package, delegates and orchestrates work. Entrypoint of exc. """
from .conf import DAGConfig
from .input_processor.input_processor import InputProcessor
from .output_producer import OutputProducer


class DetServiceGenerator():
    """ Central mediating class, entrypoint of execution. """

    def __init__(self, config: DAGConfig):
        self._config: DAGConfig = config

    def run(self) -> None:
        """ Run extraction, processing and generation of signatures, changes and apps. """
        input_processor = InputProcessor(self._config)
        activities = input_processor.get_activities()
        discretization = input_processor.get_discretization()
        output_producer = OutputProducer(discretization, self._config)
        for activity in activities:
            output_producer.write_signature(activity)
            output_producer.write_changes(activity)
            output_producer.write_app(activity)
        output_producer.write_log()
