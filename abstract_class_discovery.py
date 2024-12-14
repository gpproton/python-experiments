# sample
import abc


class BaseTask(abc.ABC):
    prop_one: str = "sample_prop"

    def __init__(self) -> None:
        super().__init__()
        print("constructor => {0}".format(self.__class__.__name__))

    @abc.abstractmethod
    def trigger_action(self):
        pass


class TaskOne(BaseTask):
    def trigger_action(self):
        print("task-one")
        print("display props => {0}".format(self.prop_one))


class TaskTwo(BaseTask):
    def trigger_action(self):
        print("task-two")


if __name__ == "__main__":
    child_classes = BaseTask.__subclasses__()

    for child_class in child_classes:
        instance = child_class()
        instance.trigger_action()
