#!/bin/bash

CLI=./cli.py

$CLI drop
$CLI init
$CLI insert views json test_events.json
$CLI insert timestamp_summary json test_summaries.json
