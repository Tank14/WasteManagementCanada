<h1>WasteManagementCanada</h1>
<h4>WWI Summer School 17</h4>

<h2>Use-Case Description:</h2>
<b>Problem</b> <br>
Waste Management is an integral part of every municipality. Most current systems lack insight into their management solutions. Without insight into live statistics about different conditions of the waste produced in homes, the system is handicapped as to how efficient it can be, leading to a lot of wasted resources and labor hours.

<b>Solution</b> <br>
Our Smart Waste Management Solution resolves this problem. By leveraging IoT we are now able to collect live statistics of everyday household items. Our solution implements sensors in each household garbage bin to report live statistics about its conditions. Knowing whether a garbage bin is full, empty, or how long it has been since it was last collected can help optimize daily collection routes. Doing this provides not only financials savings to the company, but also a more environmentally friendly solution. Please view our presentation for more details and how the Smart Waste Management Solution can benefit you.</br>
Build the project below to view a prototype implementation of this solution. 

<h2>Building</h2>

<b>Requirements:</b></br>
-Python 2.7<br>
-Spark 1.6 w/ Hadoop 2.6<br>
-PySpark<br>

Note: This project has been tested under Ubuntu 14.04.

<b>Steps:</b><br>
1. Clone this project<br>
2. Run the Project.py file: pyspark Project.py <br> 
3. Run the script to update the Heat Map with the new data: sh format_heatmap_source.sh<br>
4. Open /Leaflet/index.html to view a Heat Map based on new data<br>

<b>Modifications</b><br>
Source File:<br>
-Under the source/ folder there are different sized samplesets to be used for the system optimization<br>
-Modify line 16 in Project.py with whichever source file you would like to run the test against<br><br>

Note: This is a prototype. Statistics are currently displayed in the console. For a production environment, the solution would output data for further processing.

<h2>Presentation</h2>
For the HTML5 presentation, open "index.html" in the "presentation HTML5"-folder in oder to see the presentation.
