
import json
import uuid
import argparse
import os
import re

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
                  "producer": "https://meltano.com"
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
                  "producer": "https://meltano.com"
                }

  return start_record, end_record


def convert_dict_to_array(d):
  for key, value in d.items():

    if isinstance(value["type"], list):
      value["type"] = value["type"][0]

    item = {
      "name": key,
      "type": value["type"],
      "description": value.get("description")
    }
    yield item


def parse_logs(filepath, manifest_path):

  f = open(filepath, "r")
  m = json.load(open(manifest_path, "r"))

  for line in f.readlines():
    j = json.loads(line)

    if j.get("event") and re.match(r"Environment (.*) is active", j["event"]):
        d = {
          "producer": "https://meltano.com",
          "producer_name": None, 
          "consumer_name": None, 
          "streams": [],
          "inputs": [],
          "outputs": [],
          "run": {
            "runId": str(uuid.uuid4()),
            "metrics": {}
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
    
    if j.get("timestamp") and not d["start_time"]:
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
        current_metric = d["run"]["metrics"].get(metric_name) or 0
        d["run"]["metrics"][metric_name] = current_metric + metric_value

    if j.get("event").startswith('{\"type\": \"SCHEMA\"'):
      
      name = j["string_id"]
      parsed_json = json.loads(j.get("event"))

      input = {
          "namespace": "meltano",
          "name": name,
          "facets": {
            "schema": {
              "fields": []
            },
            "config": None
          }
      }

      stream_name = parsed_json["stream"]
      if not stream_name in d["streams"]:
        d["streams"].append(stream_name)
        schema = list(convert_dict_to_array(parsed_json["schema"]["properties"]))
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
  # start_record = {
  #                 "eventType": "START", 
  #                 "eventTime": d["start_time"],
  #                 "run": {
  #                   "runId": d["run"]["runId"],
  #                 },
  #                 "inputs": d["inputs"],
  #                 "outputs": d["outputs"]
  #               }

  # end_record = {
  #                 "eventType": "COMPLETE",
  #                 "eventTime": d["end_time"],
  #                 "run": d["run"],
  #                 "inputs": d["inputs"],
  #                 "outputs": d["outputs"]
  #               }



if __name__ == "__main__":
   
   print("Yay we be burning")

  # parser = argparse.ArgumentParser()
  # parser.add_argument("--logs", help="The Meltano logs file", default="data/meltano.log")
  # parser.add_argument("--manifest", help="The Meltano manifest file", default="data/manifest.json")
  # parser.add_argument("--output", help="The output file", default="output.json")
  # parser.add_argument("--print", help="Print the output to the console", default=False)

  # args = parser.parse_args()
  # results = parse_logs(args.logs, args.manifest)

  # if os.path.exists("openlinage_" + args.output):
  #   os.remove("openlinage_" + args.output)

  # if os.path.exists("meltano_" + args.output):
  #   os.remove("meltano_" + args.output)


  # openlineage_records = open("openlinage_" + args.output, "w")
  # meltano_summary = open("meltano_" + args.output, "w")

  # for result in results:
  #   meltano_summary.write(json.dumps(result) + '\n')
  #   openlineage_start, openlineage_complete = emit_openlineage_from_summary(result)
  #   openlineage_records.write(json.dumps(openlineage_start) + '\n')
  #   openlineage_records.write(json.dumps(openlineage_complete) + '\n')

  # if args.print:
  #   print("******* START RECORD: *******")
  #   print(json.dumps(start_record, indent=2))
  #   print("******* COMPLETE RECORD: *******")
  #   print(json.dumps(complete_record, indent=2))