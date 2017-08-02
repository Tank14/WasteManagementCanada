#!/bin/bash
# Note: this bash script cannot be run before running Project.py file


# add first line for heatmap source file
echo "var addressPoints = [">Leaflet/heatmap_source.js

# add the actual data
cat result/part-00000>>Leaflet/heatmap_source.js

# add the footer
echo "];">>Leaflet/heatmap_source.js
