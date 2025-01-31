const xlsx = require("xlsx");
const fs = require("fs");
const path = require("path");

// Correct the file path here
const excelFilePath = path.resolve("C:/Users/adhav/Downloads/Combined data for product page.xlsx");

// Check if the file exists
if (!fs.existsSync(excelFilePath)) {
    console.error("File does not exist:", excelFilePath);
    process.exit(1); // Exit if the file isn't found
}

// Read the Excel file
const workbook = xlsx.readFile(excelFilePath);

// Get the first sheet's name
const sheetName = workbook.SheetNames[0];

// Get the data from the specified sheet
const worksheet = workbook.Sheets[sheetName];

// Convert the sheet data to JSON
const jsonData = xlsx.utils.sheet_to_json(worksheet);

function calculateMetrics(data) {
   
    return data.map(item => {
      // Convert Excel serial number to date
      if (item.date) {
        const baseDate = new Date(1899, 11, 30); // Excel starts at 30-Dec-1899
        item.readable_date = new Date(baseDate.getTime() + item.date * 24 * 60 * 60 * 1000).toISOString().slice(0, 10); // Format: YYYY-MM-DD
      } else {
        item.readable_date = null; // Handle missing dates
      }
  
      return item;
    });
  }
  
  const result = calculateMetrics(jsonData);
  console.log(result);

// Save JSON to a file (optional)
fs.writeFileSync("output.json", JSON.stringify(jsonData, null, 2), "utf-8");

console.log("Excel data converted to JSON:");
