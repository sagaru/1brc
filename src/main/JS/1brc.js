/* The task is to write a Java program which reads the file, calculates the min, mean, and max temperature value per weather station, 
and emits the results on stdout like this (i.e. sorted alphabetically by station name, 
and the result values per station in the format <min>/<mean>/<max>, rounded to one fractional digit):
{Abha=-23.0/18.0/59.2, Abidjan=-16.2/26.0/67.3, Abéché=-10.0/29.4/69.0, Accra=-10.1/26.4/66.4, Addis Ababa=-23.7/16.0/67.0, Adelaide=-27.8/17.3/58.5, ...}
*/

const fs = require("fs");
// to parse large file
const csv = require("csv-parser");
const filePath = "1brc/weather_stations.csv";

var startTime = performance.now()
// Create an array to store the processed data
const processedData = {};

function avg(values) {
  let sum = 0;
  // without parseFloat all negative int will have NaN value
  values.forEach((value) => {
    sum += parseFloat(value);
  });
  return sum / values.length;
}

function readLargeFile(filePath) {
  const data_stream = fs.createReadStream(filePath);
  data_stream
    .pipe(csv())
    .on("data", (row) => {
      // Process each row here
      let line = Object.entries(row)[0][1];
      // split by ;
      line = line.split(";");
      if (line.length === 2) {
        key = line[0];
        value = line[1];
        // if the key is not present add a new entry to the unique station
        if (!processedData[key]) {
          processedData[key] = [];
        }

        // if the key is present add values to it
        processedData[key].push(value);
      }
    })
    .on("end", () => {
      //console.log("CSV file successfully processed");
      //Sort them and print them in the asked format
      Object.keys(processedData)
        .sort()
        .forEach((station) => {
          console.log(
            station +
              "=" +
              parseFloat(processedData[station].sort()[0]).toFixed(1) +
              "/" +
              parseFloat(avg(processedData[station])).toFixed(1) +
              "/" +
              parseFloat(
                processedData[station].sort()[processedData[station].length - 1]
              ).toFixed(1)
          );
        });
        var endTime = performance.now()
        console.log(`Time to run :: ${endTime - startTime} milliseconds`) // 321 ms
    })
    .on("error", (error) => {
      console.error("Error processing CSV file:", error);
    });
}
readLargeFile(filePath);
