#!/bin/bash
spark-submit --deploy-mode client --class gumbo.gui.GumboGUI --master yarn --driver-memory 512M --executor-memory 512M --executor-cores 2 ./gumbo.jar 
