import apache_beam as beam
import csv
from utils.config import input_file, output_file 

def my_transformation(element):
    # Replace this with your actual transformation logic
    # Example: Convert the element to uppercase
    transformed_data = element.upper()
    return transformed_data

def run():
    with beam.Pipeline() as p:
        (p
         | 'ReadInput' >> beam.io.ReadFromText(input_file)
         | 'TransformData' >> beam.Map(my_transformation)
         | 'WriteOutput' >> beam.io.WriteToText(output_file, file_name_suffix='.csv')
        )

if __name__ == '__main__':
    run()
