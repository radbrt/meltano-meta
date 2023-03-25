# OpenLineage parser for Meltano

This is a thinking-out-loud kind of project, the few lines of code here are meant as a starting point to trigger some thoughts, not a proposed solution.

## The goal

Meltano loads data from sources to destination, but there is currently little integration with lineage. It should, however, be possible to output some open-lineage type metadata.

## What is possible today

Based on what is available today, we are able to extract relevant information from two places:
  - the meltano run logs
  - the manifest (or directly from meltano.yml)

The low-hanging fruit available regardless of tap and target, are the following:
  - run start time
  - run end time
  - outcome (success/error)
  - name of source and destination (meltano reference name, but not a technical reference to the source)
  - names and schema of the streams

With SDK-based taps/targets, there are also metrics logged, so that we for each run can add number of rows processed etc.

## Incremental improvements

Many of the taps/targets have commonalities in their configuration, and with some custom coding we may be able to create generic references to source/target data. This will, of course, always be approximations, but for databases in particular it is possible to construct references following the openlineage nomenclature like `mysql://db.foo.com:6543/metrics.orders`. For a number of taps/targets, like APIs and some file locations, this will be inexact at best.


## Improvements to meltano and the SDK

Even when writing logs as JSON, the format is less than ideal. Especially the "metrics" is basically a json as text inside a longer text string. Not really a problem, but...

The logs can also be more streamlined to fit with the open-lineage specification.

Separate json-lines for "started", "completed" and "error" should be fairly low-hanging fruit and would make parsing a lot more predictable. In general, custom log-lines for lineage/observability should be doable.

I don't know how difficult it would be to arrange a separate function in meltano for writing directly to a marquez endpoint, but I assume it is possible for a target to be marquez-enabled.


## What the script does

The `data` folder contains two log-files from meltano, as well as a manifest file. These are fairly randomly assembled, and the output demonstrates that.

Basically, `produce_metadata.py` parses the manifest and a log file, and assembles one "START" and one "COMPLETED" record which by default is written to `openlineage.json`. This output should be fairly compliant with the openlineage standard.

The entire "config" field of taps/targets are appended to each stream (`input`/`output` in openlineage terminology), including config from parent taps if inheritance is used. Beware if there are hardcoded passwords etc in the config. Config defined as env variables are not included. Because the taps/target config is defined by the different taps/targets, we aren't parsing this - just sending it with. A possible improvement could be to grap the "root" tap/target used, which makes post-processing to figure out absolute references to the data possible.

The script can be run without arguments (just `python produce_metadata.py`), but it accepts the following arguments:
  - `--logs`: path to the log file to parse. Default `data/meltano.log`
  - `--manifest`: path to the manifest file to parse. Default `data/manifest.json`
  - `--output`: path to metadata file to be written. Default `openlineage.json`
  - `--print`: option to pretty-print the metadata records to the console. Default false.





