import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, WorkerOptions
from apache_beam.options.value_provider import StaticValueProvider, ValueProvider
import logging

# Configure logging to display messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class LogMessage(beam.DoFn):
    """
    A DoFn that logs each element (Pub/Sub message) it receives.
    """
    def process(self, element):
        # Pub/Sub messages are typically bytes, so decode them to string
        message_content = element.decode('utf-8') if isinstance(element, bytes) else element
        logging.info(f"Received message: {message_content}")
        return [] # Return an empty list as we are not passing anything further

class CustomPipelineOptions(PipelineOptions):
    """
    Custom pipeline options for project and subscription,
    allowing them to be provided at runtime for Flex Templates.
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add a custom argument for the Google Cloud project ID
        parser.add_argument(
            '--project_id',
            dest='project_id',
            required=True, # Make this parameter mandatory
            help='The Google Cloud Project ID.',
        )
        # Add a custom argument for the Pub/Sub subscription name
        parser.add_argument(
            '--subscription_name',
            dest='subscription_name',
            required=True, # Make this parameter mandatory
            help='The name of the Pub/Sub subscription (e.g., "my-subscription").',
        )

def run_pipeline():
    """
    Constructs and runs the Apache Beam pipeline, reading parameters
    from CustomPipelineOptions.
    """
    # Define pipeline options
    options = PipelineOptions()
    # Cast options to our custom class to access project_id and subscription_name
    custom_options = options.view_as(CustomPipelineOptions)

    # For running on Dataflow as a Flex Template, these are typical settings
    options.view_as(StandardOptions).runner = 'DataflowRunner' # Must be DataflowRunner for Flex Templates
    options.view_as(StandardOptions).streaming = True # Required for Pub/Sub source

    # Dataflow-specific options that might be set via `metadata.json` or command line
    # The `project` and `region` can also be inferred from the gcloud context
    # if not explicitly set in the template parameters or command line.
    options.view_as(GoogleCloudOptions).project = custom_options.project_id
    options.view_as(GoogleCloudOptions).region = 'us-central1' # Replace with your desired region
    # A temporary location in GCS for staging files. This is mandatory for DataflowRunner.
    options.view_as(GoogleCloudOptions).temp_location = f'gs://your-dataflow-bucket/temp'
    # Specify the GCS path for the staging location of the template
    options.view_as(GoogleCloudOptions).staging_location = f'gs://your-dataflow-bucket/staging'


    # Create a pipeline object
    with beam.Pipeline(options=options) as pipeline:
        # Define the Pub/Sub subscription path.
        # ValueProviders handle deferred access, meaning the actual value is resolved at runtime.
        full_subscription_path = custom_options.project_id.get() # .get() retrieves the underlying value
        full_subscription_path = f'projects/{full_subscription_path}/subscriptions/{custom_options.subscription_name.get()}'

        # Read messages from Pub/Sub
        messages = pipeline | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=full_subscription_path)

        # Apply the DoFn to log each message
        messages | "LogMessages" >> beam.ParDo(LogMessage())

        logging.info("Pipeline started. Waiting for messages...")

if __name__ == '__main__':
    # When running as a Flex Template, the parameters will be provided by Dataflow.
    # For local testing, you can pass them as command-line arguments:
    # python your_pipeline_file.py --project_id your-gcp-project-id --subscription_name your-pubsub-subscription-name
    logging.info("Starting pipeline execution...")
    run_pipeline()