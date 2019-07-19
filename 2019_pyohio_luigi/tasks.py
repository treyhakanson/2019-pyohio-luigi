"""Luigi tasks."""
import json
import csv
from abc import ABC

import luigi
from luigi.contrib import postgres
from luigi.util import requires, inherits, common_params


class ResourceTask(luigi.Task):
    """Task to require the existence of a file."""

    fname = luigi.Parameter()

    def requires(self):
        # This task requires that the file exist locally; could be elsewhere,
        # like AWS s3 (luigi.contrib.aws.S3Target) or
        # GCS (luigi.contrib.gcs.GCSTarget)
        return luigi.LocalTarget(self.fname)


@requires(ResourceTask)
class AbstractParseTask(ABC, luigi.Task):
    pass


class ParseJSONTask(AbstractParseTask):
    """Task to parse a JSON array of objects into a consistent format."""

    def run(self):
        # Format the input, and write to the output
        with self.input().open() as f:
            data = json.load(f)
        header = list(data[0].keys())
        parsed_data = [[i, *obj.values()] for i, obj in enumerate(data)]
        with self.output().open(mode="w") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(parsed_data)

    def output(self):
        # This task writes its output to a local file
        return luigi.LocalTarget()


class ParseCSVTask(AbstractParseTask):
    """Task to parse a CSV into a consistent format."""

    def run(self):
        # Format the input, and write to the output
        with self.input().open() as f:
            reader = csv.reader(f)
            parsed_data = [[i, *row] for i, row in enumerate(reader)]
        with self.output().open(mode="w") as f:
            writer = csv.writer(f)
            writer.writerows(parsed_data)

    def output(self):
        # This task writes its output to a local file
        return luigi.LocalTarget()


@inherits(AbstractParseTask)
class ParseTask(luigi.Task):
    """Parse a CSV or JSON resource into a consistent format."""

    def run(self):
        # Naively check file type
        ext = self.input().path.split(".")[-1]
        params = common_params(self, AbstractParseTask)
        if ext == "csv":
            yield ParseCSVTask(**params)
        elif ext == "json":
            yield ParseJSONTask(**params)
        else:
            raise Exception("Not sure how to handle that file type...")


@requires(ParseTask)
class TransformTask(luigi.Task):
    """Transform the consistently formatted data."""

    def run(self):
        with self.input().open() as f:
            reader = csv.reader(f)
            data = list(reader)

        # Do some awesome transforms...

        with self.output().open(mode="w") as f:
            writer = csv.writer(f)
            writer.writerows(data)

    def output(self):
        return luigi.LocalTarget()


@requires(TransformTask)
class UploadTask(postgres.CopyToTable):
    """Upload the data to a postgres instance."""

    host = "localhost"
    database = "my_cool_database"
    user = "luigi"
    password = "hushhush"
    table = "my_cool_table"
    columns = ("foo", "bar", "baz")

