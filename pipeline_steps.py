from typing import TypeVar, Generic, List, Any
from abc import ABC, abstractmethod

# Define generic types for input and output
InputType = TypeVar("InputType")
OutputType = TypeVar("OutputType")


class Step(ABC, Generic[InputType, OutputType]):
    @abstractmethod
    def process(self, input_data: InputType) -> OutputType:
        pass


class Pipeline:
    def __init__(self) -> None:
        self.steps: List[Step[Any, Any]] = []

    def add_step(self, step: Step[InputType, OutputType]) -> None:
        if self.steps:
            # Validate type compatibility between last step's output and new step's input
            last_step = self.steps[-1]
            if not isinstance(last_step, Step) or not isinstance(step, Step):
                raise TypeError("Pipeline steps must inherit from the Step class.")

        self.steps.append(step)

    def run(self, input_data: Any) -> Any:
        data = input_data
        for step in self.steps:
            data = step.process(data)
        return data


# Example Step Implementations
class StringToUpper(Step[str, str]):
    def process(self, input_data: str) -> str:
        return input_data.upper()


class StringSplitter(Step[str, List[str]]):
    def process(self, input_data: str) -> List[str]:
        return input_data.split()


class WordCounter(Step[List[str], int]):
    def process(self, input_data: List[str]) -> int:
        return len(input_data)


# Example usage
if __name__ == "__main__":
    pipeline = Pipeline()
    pipeline.add_step(StringToUpper())  # Converts a string to uppercase
    pipeline.add_step(StringSplitter())  # Splits the string into words
    pipeline.add_step(WordCounter())  # Counts the number of words

    input_text = "hello world from pipeline"
    result = pipeline.run(input_text)
    print(f"Result: {result}")  # Output: Result: 4
