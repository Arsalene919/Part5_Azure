import azure.functions as func
import azure.durable_functions as df
import logging

def main(Reducerinput: dict) -> dict:
    key = Reducerinput["key"]
    values = Reducerinput["values"]
    return {key: sum(values)}
