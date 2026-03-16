## Tests
How confident are you in the code you work on? Will it do the things it is supposed to do as expected? Do you think that you can confidently modify this code without breaking any of its current functionalities? If this code is not properly tested, you probably don't, and shouldn't. If it is actually tested then maybe you can trust it.

Testing the code we write is one of the most valuable tool of software engineering. Tests help to ascertain that the functionalities of our code work as expected, they help to find out bugs even before they happen, they help make our code easy to change and they will even directly influence the design of your software, especially if you use Test Driven Development.

## Manual tests

Manual tests are the first kind of tests we do and they are commonly used. When you develop something new you try to make it run, manually. Once it looks like it's working you are happy and you might decide to move on. However, you can only test a limited amount of cases like this before it gets too expensive and time consuming. As your codebase grows you probably won't test manually all its older functionalities, but only the one you are currently working on. This is why we advocate to adopt automated tests as the first tests to implement.

## Automated tests

Automated tests are tests that are executed by a computer and thus they can be part of a development pipeline. For now, we will only talk about unit tests which are used to test the low-level functionalities of your code, the direct results of the implementation your are working on. In Nexus-e we use Python pytest framework, a pytest test looks like this:

```python
class TestValueFormat:
    def test_truncate_value_to_decimal(self):
        # Arrange
        sut = ValueFormatter(0.12345)
        truncated_decimal = 3
        expected_result = 0.123

        # Act
        result = sut.truncate(decimal=truncated_decimal).get_formatted_value()

        # Assert
        assert result == expected_result
```

To apply different test cases on the same test you can parametrize the test:

```python
class TestValueFormat:
    truncate_value_data = [
        (0.12345, 3, 0.123),
        (0.1234567, 6, 0.123456),
        (0.12, 3, 0.12),
    ]

    @pytest.mark.parametrize(
        "value, decimal, expected_result", truncate_value_data
    )
    def test_truncate_value_to_decimal(self, value, decimal, expected_result):
        # Arrange
        sut = ValueFormatter(value)

        # Act
        result = sut.truncate(decimal=decimal).get_formatted_value()

        # Assert
        assert result == expected_result
```

Such test shouldn't replicate the logic of the production code which means the expected result should be hard-coded or read from a source and not obtained with the same algorithm that runs in the code you want to test.

## F.I.R.S.T. principles of testing
These principles help to write clean tests:

### Fast
The execution of the tests should be fast. If it's not, you will likely avoid executing your tests too often. If you don't run your tests often enough you will let bugs spread and you will be less confident in modifying the codebase, countering the benefits of testing.

### Independent
Tests should be independent from each other. One test shouldn't define the conditions of execution of another test. We should be able to run our tests in any given order.

### Repeatable
Tests should be deterministic and their results shouldn't be influenced by the environment they run in. Each test should set up its own data and should not depend on any external factors to run.

### Self-validating
Tests should provide a binary result: either the test pass or it fails. We shouldn't need to analyze some data produced by a test to determine if it is successful.

### Timely
Tests should be written at the appropriate time, which is right before writing the actual production code.

## Arrange, Act, Assert
It is a proven practice to divide a test in three parts.

### Arrange
In this part you setup your System Under Test (SUT). It can implies instanciating the component you want to test and the other variables needed in the test.

### Act
In this part you run the functionality you want to test. A test should focus on only one functionality. At this point you might obtain a result from the tested functionality.

### Assert
Here you assert that the computed result corresponds to an expected result. You might also assert that some expected behavior have been observed (calling a method, raising an exception, ...).

## Where should I write my tests
In the root folder of nexus-e-framework exists a "tests" folder. Write your unit tests in files like test>unit_tests>test_my_component.py. The organization of the tests files should reproduce the organization of the production code. 