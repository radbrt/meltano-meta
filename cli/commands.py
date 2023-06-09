import hashlib
import json
import logging
import os
import re
import subprocess

import click
import requests
import yaml

LOGGER = logging.getLogger(__name__)


def find_element(name, data):
    for element in data:
        if element["name"] == name:
            return element
    return None


def get_values(name, data):
    element = find_element(name, data)
    if not element:
        return {}

    values_ref = element.get("config") or {}
    values = values_ref.copy()
    if element.get("inherit_from"):
        parent_values = get_values(element.get("inherit_from"), data)
        parent_values.update(values)
        values = parent_values

    return values


def convert_dict_to_array(d):
    for key, value in d.items():
        if isinstance(value["type"], list):
            value["type"] = value["type"][0]

        item = {
            "name": key,
            "type": value["type"],
            "description": value.get("description"),
        }
        yield item


def emit_openlineage_from_summary(run_summary):
    start_record = {
        "eventType": "START",
        "eventTime": run_summary["start_time"],
        "run": {
            "runId": run_summary["run"]["runId"],
        },
        "job": run_summary["job"],
        "inputs": run_summary["inputs"],
        "outputs": run_summary["outputs"],
        "producer": "https://meltano.com",
    }

    if run_summary["success"]:
        event_type = "COMPLETE"
    else:
        event_type = "FAIL"

    end_record = {
        "eventType": event_type,
        "eventTime": run_summary["end_time"],
        "job": run_summary["job"],
        "run": run_summary["run"],
        "inputs": run_summary["inputs"],
        "outputs": run_summary["outputs"],
        "producer": "https://meltano.com",
    }

    return start_record, end_record


def parse_logs(filepath, m):
    f = open(filepath, "r")

    for i, line in enumerate(f.readlines()):
        j = json.loads(line)

        if (
            j.get("event") and re.match(r"Environment (.*) is active", j["event"])
        ) or i == 0:
            d = {
                "producer": "https://meltano.com",
                "producer_name": None,
                "consumer_name": None,
                "streams": [],
                "inputs": [],
                "outputs": [],
                "run": {
                    "runId": hashlib.sha256(
                        str(j.get("event") + j.get("timestamp")).encode()
                    ).hexdigest(),
                    "facets": {"metrics": {}},
                },
                "job": {
                    "namespace": None,
                    "name": None,
                },
                "start_time": None,
                "end_time": None,
            }

        if j.get("producer"):
            d["producer_name"] = j["string_id"]

        if j.get("consumer"):
            d["consumer_name"] = j["string_id"]

        if j.get("timestamp") and not d.get("start_time"):
            d["start_time"] = j["timestamp"]
        if j.get("timestamp"):
            d["end_time"] = j["timestamp"]

        if "INFO METRIC:" in j.get("event"):
            config = get_values(j["string_id"], m["plugins"]["loaders"])
            name = j["string_id"]

            start_pos = j["event"].find("INFO METRIC:") + len("INFO METRIC: ")
            json_string = j["event"][start_pos:]
            parsed_json = json.loads(json_string)
            metric_type = parsed_json["metric_type"]
            metric_name = parsed_json["metric"]
            metric_value = parsed_json["value"]
            if metric_type in ["timer", "counter", "sync_duration"]:
                current_metric = d["run"]["facets"]["metrics"].get(metric_name) or 0
                d["run"]["facets"]["metrics"][metric_name] = (
                    current_metric + metric_value
                )

        if j.get("event") and "MELTANO-META-LOGGER" in j.get("event"):
            json_part = re.search(r"\{.*\}", j.get("event")).group(0)

            # Parse the JSON part into a Python dictionary
            parsed_json = json.loads(json_part)

            if j.get("consumer") and parsed_json.get("schema"):
                uri = parsed_json["uri"]
                schema = list(
                    convert_dict_to_array(parsed_json["schema"]["properties"])
                )
                output = {
                    "namespace": "meltano",
                    "name": uri,
                    "facets": {"schema": {"fields": schema}, "config": None},
                }
                output["facets"]["schema"]["_producer"] = "meltano"
                output["facets"]["schema"]["_schemaURL"] = uri
                d["outputs"].append(output)

            if j.get("producer") and parsed_json.get("schema"):
                uri = parsed_json["uri"]
                schema = list(
                    convert_dict_to_array(parsed_json["schema"]["properties"])
                )
                input = {
                    "namespace": "meltano",
                    "name": parsed_json["table_name"],
                    "facets": {"schema": {"fields": schema}, "config": None},
                }
                input["facets"]["schema"]["_producer"] = "meltano"
                input["facets"]["schema"]["_schemaURL"] = uri
                d["inputs"].append(input)

        if j.get("event").startswith('{"type": "SCHEMA"'):
            name = j["string_id"]
            parsed_json = json.loads(j.get("event"))

            input = {
                "namespace": "meltano",
                "name": name,
                "facets": {"schema": {"fields": []}, "config": None},
            }

            stream_name = parsed_json["stream"]
            if stream_name not in d["streams"]:
                d["streams"].append(stream_name)
                schema = list(
                    convert_dict_to_array(parsed_json["schema"]["properties"])
                )
                input["name"] = stream_name
                input["facets"]["schema"]["_producer"] = "meltano"
                input["facets"]["schema"]["_schemaURL"] = "https://example.com"
                input["facets"]["schema"]["fields"] = schema

                if j.get("producer"):
                    config = get_values(name, m["plugins"]["extractors"])
                    input["facets"]["config"] = config.copy()
                    d["inputs"].append(input)
                if j.get("consumer"):
                    config = get_values(name, m["plugins"]["loaders"])
                    input["facets"]["config"] = config.copy()
                    d["outputs"].append(input)

        if "success" in j.keys():
            d["success"] = j["success"]
            d["job"]["namespace"] = "meltano"
            d["job"]["name"] = f"{d['producer_name']}-to-{d['consumer_name']}"
            yield d

    f.close()


def find_logfile():
    y = yaml.safe_load(open("logging.yaml", "r"), Loader=yaml.FullLoader)

    for k, v in y["handlers"].items():
        if "filename" in v.keys() and v["formatter"] == "json":
            yield v["filename"]


def get_configs(environment=None):
    if environment:
        location = f".meltano/manifests/meltano-manifest.{environment}.json"
    else:
        location = ".meltano/manifests/meltano-manifest.json"
    if not os.path.exists(location):
        raise FileNotFoundError(f"Manifest file {location} not found")

    m = json.load(open(location, "r"))

    return m


def post_to_marquez(url, data):
    headers = {"Content-type": "application/json"}

    # Add bearer authentication if token is provided
    if os.getenv("MARQUEZ_API_KEY"):
        auth_token = os.getenv("MARQUEZ_API_KEY")
        headers["Authorization"] = f"Bearer { auth_token }"

    r = requests.post(url, json=data, headers=headers, timeout=10)

    if not r.ok:
        raise Exception(f"Error posting to OpenLineage: {r.status_code}, {r.text}")


@click.command()
@click.option("--environment", "-e", default=None, help="environment to search in")
@click.option(
    "--url",
    default="http://localhost:5000/api/v1/lineage",
    help="OpenLineage URL endpoint",
)
@click.option("--publish", default=False, help="Publish to OpenLineage", is_flag=True)
@click.option("--outfile", "-o", default=None, help="Output file")
def logparser(environment, url, publish, outfile):
    if not environment:
        LOGGER.info(
            "No environment provided, using default_environment from meltano.yml"
        )
        projectfile = yaml.safe_load(open("meltano.yml", "r"), Loader=yaml.FullLoader)
        environment = projectfile["default_environment"]

    if not os.path.exists(f".meltano/manifests/meltano-manifest.{environment}.json"):
        LOGGER.info(
            f"Manifest file .meltano/manifests/meltano-manifest.{environment}.json not found, invoking `meltano compile`"
        )
        compile_result = subprocess.run(["meltano", "compile"])
        if not compile_result.returncode == 0:
            raise Exception(
                f"Error invoking meltano compile: {compile_result.returncode}"
            )

    configs = get_configs(environment)
    for file in find_logfile():
        logs = parse_logs(file, configs)

    openlineage_logs = [emit_openlineage_from_summary(summary) for summary in logs]

    if publish:
        for start_entry, end_entry in openlineage_logs:
            post_to_marquez(url, start_entry)
            post_to_marquez(url, end_entry)

    if outfile:
        with open(outfile, "w") as f:
            for start_entry, end_entry in openlineage_logs:
                f.write(json.dumps(start_entry) + "\n")
                f.write(json.dumps(end_entry) + "\n")

    if not outfile and not publish:
        for start_entry, end_entry in openlineage_logs:
            print(json.dumps(start_entry))
            print(json.dumps(end_entry))


def get_configs_from_file(location):
    if not os.path.exists(location):
        raise FileNotFoundError(f"Manifest file {location} not found")

    m = json.load(open(location, "r"))
    return m


@click.command()
@click.option("--file", "-f", default=None, help="Logfile to parse")
@click.option("--manifest", "-m", default=None, help="Manifest file to use")
def parsefile(file, manifest):
    if not file:
        raise Exception("No file provided")

    if not os.path.exists(file):
        raise FileNotFoundError(f"File {file} not found")

    if not manifest:
        raise Exception("No manifest file provided")

    if not os.path.exists(manifest):
        raise FileNotFoundError(f"Manifest file {manifest} not found")

    configs = get_configs_from_file(manifest)
    logs = parse_logs(file, configs)
    openlineage_logs = [emit_openlineage_from_summary(summary) for summary in logs]

    for start_entry, end_entry in openlineage_logs:
        print(json.dumps(start_entry))
        print(json.dumps(end_entry))
