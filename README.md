# OpenLineage parser for Meltano

This is a simple CLI for parsing meltano logs, subject to a lot of breaking changes and improvements.

## The goal

Meltano loads data from sources to destination, but there is currently little integration with lineage. It should, however, be possible to output some open-lineage type metadata. This cli tool parses key files in a Meltano project, and creates openlineage-compliant records. It can optionally post these records to a Marquez API endpoint.

## What it currently does

Currently, the tool finds the following information:
- the name of the tap and target
- the name of the stream
- The start and end time for the run
- The schema (of the source at least)
- run metrics (number or rows etc) if the tap/target is SDK-based
- Run result (success/error)
- Tap/Target config such as host

## What might be possible

Perhaps the biggest improvement of this tool would be to automatically generate URIs of the source/target locations. Such URIs typically look something like `snowflake://xy12345.north-europe.azure/db-name/schema-name/table-name` or `sftp://ftp.example.com/data-folder/file1.csv`. It is not possible to reliably parse data like that from the config, so the practical solution is probably to encourage some standardization of log messages so that the tap/target can emit these URIs themselves, to be picked up by the log parser.

It might also be possible to provide exact column name mapping. This would have to be a two-fold process:
- Mappers can create new columns, and we would need to parse these in order to calculate new columns
- targets can change the names of columns, typically in order to comply with conventions or requirements in the target system

The log parser should be able to parse mappings, but in order to catch column renamings in the target, we would require logging standards.


## Improvements to meltano and the SDK

It is obvious that a good lineage parser requires good logging and good standardization. Some ideas so far:
- Meltano could generate even clearer messages for when a run starts and when it ends
- We need to agree on some standard (optional) log fields that can be parsed for lineage purposes
- Perhaps dedicated "lineage log lines" could be useful

## How to use the CLI

When standing inside a meltano project, run `meltano-lineage` to parse the logs and generate summary output. By default this output will be printed to stdout, but it can optionally be written directly to file by passing a filename to the `--output` or `-o` option, or it can publish lineage directly to a Marquez endpoint by using the `--publish` flag.

The script can be run without arguments (just `meltano-lineage`), but it accepts the following arguments:
  - `--environment`: Which meltano environment to use. Defaults to the project default.
  - `--oufile`: path to metadata file to be written. Default `openlineage.json`
  - `--publish`: flag to post the records to a Marquez endpoint, specified by the `--url` option.
  - `--url`: A Marquez endpoint to post records to. Default `http://localhost:5000/api/v1/lineage`. Not used without the `--publish` flag.


## Requirements

In order for the parser to work, you need a Meltano project with a `logging.yaml` file that specifies logging to file with JSON formatting and at least INFO level logging. Because the tool parses logs, `meltano run` must have been invoked first.



