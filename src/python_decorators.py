# Databricks notebook source
#decorator to measure function execution time

import time
def measure_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Function {func.__name__} took {execution_time:.4f} seconds to execute")
        return result
    return wrapper


@measure_execution_time
def calculate_multiply(numbers):
    tot = 1
    for x in numbers:
        tot *= x
    return tot

result = calculate_multiply([1, 2, 3, 4, 5])
print("Result:", result)


# COMMAND ----------

#decorator for function logging

def add_logging(func):
    def wrapper(*args, **kwargs):
        print(f"Calling {func.__name__} with args: {args}, kwargs: {kwargs}")
        result = func(*args, **kwargs)
        print(f"{func.__name__} returned: {result}")
        return result
    return wrapper


@add_logging
def add_numbers(x, y):
    return x + y


result = add_numbers(200, 300)
print("Result:", result)


# COMMAND ----------

def identity_decorator(func):
    def wrapper(first_name, middle_name, last_name):
        first_name = first_name.capitalize()
        middle_name = middle_name.capitalize()
        last_name = last_name.capitalize()
        full_name = f"{first_name} {middle_name} {last_name}"
        return func(full_name)
    return wrapper

@identity_decorator
def identity(full_name):
    return full_name

print(identity("p", "eswara", "kumar"))

