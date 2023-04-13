from try_art_app import get_application_data
from decouple import config
import requests
import json
import pandas as pd


def calculate(filename, column):

    df = pd.read_csv(filename + ".csv")

    df[column] = df[column].str.replace("State", "", regex=True)

    df[column] = df[column].str.title()
    df[column] = df[column].str.rstrip(" ")
    df[column] = df[column].str.lstrip(" ")

    grouped = df[column].value_counts().rename_axis(column).reset_index(name="counts")

    grouped.to_csv(column + "_sorting.csv", index=False)


if __name__ == "__main__":
    get_application_data()
    calculate(filename="Application_data", column="ownerState")
