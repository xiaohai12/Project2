#!/bin/bash

./spark-2.2.1-bin-hadoop2.7/bin/spark-submit --class streaming.Main ./jars/cs422-project2_2.11-0.1.0.jar /stream_input 5 5 approx
