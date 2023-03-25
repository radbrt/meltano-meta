
import json
import uuid
import argparse
import os

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


def parse_logs(filepath, manifest_path):

  d = {
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

  f = open(filepath, "r")
  m = json.load(open(manifest_path, "r"))

  streams = []

  for line in f.readlines():
    j = json.loads(line)
    if j.get("producer"):
      d["producer_name"] = j["string_id"]
    if j.get("consumer"):
      d["consumer_name"] = j["string_id"]
    
    if j.get("timestamp") and not d["start_time"]:
      d["start_time"] = j["timestamp"]
    if j.get("timestamp"):
      d["end_time"] = j["timestamp"]

    if j.get("producer"):
      producer_config = get_values(j["string_id"], m["plugins"]["extractors"])
      producer_name = j["string_id"]
      
    if j.get("consumer"):
      consumer_config = get_values(j["string_id"], m["plugins"]["loaders"])
      consumer_name = j["string_id"]

    
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
        schema = parsed_json["schema"]["properties"]
        input["name"] = stream_name
        input["facets"]["schema"]["fields"] = schema

        if j.get("producer"):
          config = get_values(name, m["plugins"]["extractors"])
          input["facets"]["config"] = config.copy()
          d["inputs"].append(input)
        if j.get("consumer"):
          config = get_values(name, m["plugins"]["loaders"])
          input["facets"]["config"] = config.copy()
          d["outputs"].append(input)

  f.close()
  start_record = {
                  "eventType": "START", 
                  "eventTime": d["start_time"],
                  "run": {
                    "runId": d["run"]["runId"],
                  },
                  "inputs": d["inputs"],
                  "outputs": d["outputs"]
                }

  end_record = {
                  "eventType": "COMPLETE",
                  "eventTime": d["end_time"],
                  "run": d["run"],
                  "inputs": d["inputs"],
                  "outputs": d["outputs"]
                }


  return start_record, end_record

if __name__ == "__main__":

  parser = argparse.ArgumentParser()
  parser.add_argument("--logs", help="The Meltano logs file", default="data/meltano.log")
  parser.add_argument("--manifest", help="The Meltano manifest file", default="data/manifest.json")
  parser.add_argument("--output", help="The output file", default="openlineage.json")
  parser.add_argument("--print", help="Print the output to the console", default=False)

  args = parser.parse_args()
  start_record, complete_record = parse_logs(args.logs, args.manifest)

  if os.path.exists(args.output):
    os.remove(args.output)

  with open(args.output, "w") as f:
    f.write(json.dumps(start_record) + '\n')
    f.write(json.dumps(complete_record) + '\n')

  if args.print:
    print("******* START RECORD: *******")
    print(json.dumps(start_record, indent=2))
    print("******* COMPLETE RECORD: *******")
    print(json.dumps(complete_record, indent=2))