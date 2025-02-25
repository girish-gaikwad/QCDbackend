// Import the required BigQuery library
const express = require("express");
const cors = require("cors");
const fs = require("fs");
const { BigQuery } = require("@google-cloud/bigquery");
 const dotenv = require('dotenv').config();
const path = require("path");

let credentials;
try {
  if (!process.env.GOOGLE_CLOUD_CREDENTIALS) {
    throw new Error('GOOGLE_CLOUD_CREDENTIALS environment variable is not set');
  }
  
  credentials = JSON.parse(
    Buffer.from(process.env.GOOGLE_CLOUD_CREDENTIALS, "base64").toString("utf8")
  );
} catch (error) {
  console.error('Error parsing Google Cloud credentials:', error);
  process.exit(1); // Exit the application if credentials are invalid
}
const bigqueryClient = new BigQuery({
  projectId: "hopeful-history-405018",
  credentials,
});

const app = express();
const port = 5001;

app.use(cors());
app.use(express.json());

app.get("/", (req, res) => {
  res.send("Hello, world!");
});

function groupedDataSummary(data) {
  data.forEach((value) => {
    if (value["total_products"] > 0) {
      value["maximum_retail_price"] = Math.round(
        value["maximum_retail_price"] / value["total_products"]
      );
      value["discounted_selling_price"] = Math.round(
        value["discounted_selling_price"] / value["total_products"]
      );
      value["stock_at_darkstores"] = Math.round(
        value["stock_at_darkstores"] / value["total_products"]
      );
      value["stock_at_warehouses"] = Math.round(
        value["stock_at_warehouses"] / value["total_products"]
      );
    }
  });

  return data; // Return the modified array
}

async function groupBySubcategory(data) {
  const groupedData = data.reduce((acc, item) => {
    const subcategoryId = item.subcategory_id ?? "others";

    const categoryName = item.category_name;
    const categoryId = item.category_id;
    if (!acc[subcategoryId]) {
      acc[subcategoryId] = {
        category_name: categoryName,
        category_id: categoryId,
        subcategory_id: subcategoryId,
        subcategory_name: item.subcategory_name,
        maximum_retail_price: 0,
        discounted_selling_price: 0,
        stock_at_darkstores: 0,
        stock_at_warehouses: 0,
        total_orders: 0,
        total_mrp_revenue: 0,
        total_final_revenue: 0,
        ad_spend: 0,
        ad_impressions: 0,
        ad_clicks: 0,
        ad_add_to_carts: 0,
        ad_orders: 0,
        ad_orders_othersku: 0,
        ad_orders_samesku: 0,
        ad_revenue: 0,
        total_products: 0,
      };
    }

    // Corrected Summation
    acc[subcategoryId].maximum_retail_price += item.maximum_retail_price || 0;
    acc[subcategoryId].discounted_selling_price +=
      item.discounted_selling_price || 0;
    acc[subcategoryId].stock_at_darkstores += item.stock_at_darkstores || 0; // Fixed
    acc[subcategoryId].stock_at_warehouses += item.stock_at_warehouses || 0;
    acc[subcategoryId].total_orders += item.total_orders || 0;
    acc[subcategoryId].total_mrp_revenue += item.total_mrp_revenue || 0;
    acc[subcategoryId].total_final_revenue += item.total_final_revenue || 0;
    acc[subcategoryId].ad_spend += item.ad_spend || 0;
    acc[subcategoryId].ad_impressions += item.ad_impressions || 0;
    acc[subcategoryId].ad_clicks += item.ad_clicks || 0;
    acc[subcategoryId].ad_add_to_carts += item.ad_add_to_carts || 0;
    acc[subcategoryId].ad_orders += item.ad_orders || 0;
    acc[subcategoryId].ad_orders_othersku += item.ad_orders_othersku || 0;
    acc[subcategoryId].ad_orders_samesku += item.ad_orders_samesku || 0;
    acc[subcategoryId].ad_revenue += item.ad_revenue || 0;
    acc[subcategoryId].total_products += 1;

    return acc;
  }, {});

  return await groupedDataSummary(Object.values(groupedData));
}

async function groupByCategory(data) {
  const groupedData = data.reduce((acc, item) => {
    const categoryId = item.category_id ?? "others";

    const categoryName = item.category_name;

    if (!acc[categoryId]) {
      acc[categoryId] = {
        category_name: categoryName,
        category_id: categoryId,
        // subcategory_id: [],
        // subcategory_name: [],
        maximum_retail_price: 0,
        discounted_selling_price: 0,
        stock_at_darkstores: 0,
        stock_at_warehouses: 0,
        total_orders: 0,
        total_mrp_revenue: 0,
        total_final_revenue: 0,
        ad_spend: 0,
        ad_impressions: 0,
        ad_clicks: 0,
        ad_add_to_carts: 0,
        ad_orders: 0,
        ad_orders_othersku: 0,
        ad_orders_samesku: 0,
        ad_revenue: 0,
        total_products: 0,
      };
    }

    // Corrected Summation
    acc[categoryId].maximum_retail_price += item.maximum_retail_price || 0;
    acc[categoryId].discounted_selling_price +=
      item.discounted_selling_price || 0;
    acc[categoryId].stock_at_darkstores += item.stock_at_darkstores || 0; // Fixed
    acc[categoryId].stock_at_warehouses += item.stock_at_warehouses || 0;
    acc[categoryId].total_orders += item.total_orders || 0;
    acc[categoryId].total_mrp_revenue += item.total_mrp_revenue || 0;
    acc[categoryId].total_final_revenue += item.total_final_revenue || 0;
    acc[categoryId].ad_spend += item.ad_spend || 0;
    acc[categoryId].ad_impressions += item.ad_impressions || 0;
    acc[categoryId].ad_clicks += item.ad_clicks || 0;
    acc[categoryId].ad_add_to_carts += item.ad_add_to_carts || 0;
    acc[categoryId].ad_orders += item.ad_orders || 0;
    acc[categoryId].ad_orders_othersku += item.ad_orders_othersku || 0;
    acc[categoryId].ad_orders_samesku += item.ad_orders_samesku || 0;
    acc[categoryId].ad_revenue += item.ad_revenue || 0;
    acc[categoryId].total_products += 1;
    // acc[categoryId].subcategory_id.push(item.subcategory_id);
    // acc[categoryId].subcategory_name.push(item.subcategory_name);

    return acc;
  }, {});

  return await groupedDataSummary(Object.values(groupedData));
}

async function filterMetrics(metricsArray, desiredKeys) {
  return metricsArray.map((metric) => {
    // Filter the object to include only the desired keys
    const filteredMetric = {};
    desiredKeys.forEach((key) => {
      if (metric.hasOwnProperty(key)) {
        filteredMetric[key] = metric[key];
      }
    });
    return filteredMetric;
  });
}

async function summary(data) {
  let count = 0;
  const sumDict = {
    maximum_retail_price: 0,
    discounted_selling_price: 0,
    stock_at_darkstores: 0,
    stock_at_warehouses: 0,
    total_orders: 0,
    total_mrp_revenue: 0,
    total_final_revenue: 0,
    ad_spend: 0,
    ad_impressions: 0,
    ad_clicks: 0,
    ad_add_to_carts: 0,
    ad_orders: 0,
    ad_orders_othersku: 0,
    ad_orders_samesku: 0,
    ad_revenue: 0,
  };

  // Loop through each record and sum up values
  data.forEach((value) => {
    count++; // Count the number of records
    for (let key in sumDict) {
      sumDict[key] += value[key] || 0; // Add value, handle undefined/null safely
    }
  });
  sumDict["maximum_retail_price"] = Math.round(
    sumDict["maximum_retail_price"] / count
  );
  sumDict["discounted_selling_price"] = Math.round(
    sumDict["discounted_selling_price"] / count
  );
  sumDict["stock_at_darkstores"] = Math.round(
    sumDict["stock_at_darkstores"] / count
  );
  sumDict["stock_at_warehouses"] = Math.round(
    sumDict["stock_at_warehouses"] / count
  );
  console.log("Summary:", sumDict);
  console.log("Total records processed:", count);
  return sumDict;
}

// Query to fetch data
const query = `
  SELECT * 
  FROM \`hopeful-history-405018.Quickcommerce_final.product_sales_stock_spends_combined\`
  WHERE brand_name = 'Lifelong'
  
`;

async function fetchAndProcessData() {
  try {
    const [rows] = await bigqueryClient.query(query);

    const groupedData = await groupBySubcategory(rows);
    // const groupDataCate = await groupByCategory(rows);

    try {
      fs.writeFileSync(
        "groupedData.json",
        JSON.stringify(groupedData, null, 2),
        "utf-8"
      );
      console.log("Data successfully written to groupedData.json");
    } catch (err) {
      console.error("Error writing file:", err);
    }
  } catch (error) {
    console.error("Error executing query:", error);
  }
}

// fetchAndProcessData();

async function formatDate(dateString) {
  const date = new Date(dateString);
  return date.toISOString().split("T")[0]; // Extract YYYY-MM-DD
}

app.post("/datewises", async (req, res) => {
  let {
    from,
    to,
    cond,
    metric,
    category_names,
    offset = 0,
    limit = 10,
  } = req.body;
  // Handle missing values
  if (!from || !to) {
    return res
      .status(400)
      .json({ success: false, message: "Missing 'from' or 'to' date" });
  }
  console.log(req.body);
  console.log(from, to, cond, metric, category_names);
  cond = true;
  const fromDate = await formatDate(from);
  const toDate = await formatDate(to);
  console.log(fromDate, toDate);
  const query = `
  SELECT * 
  FROM \`hopeful-history-405018.Quickcommerce_final.product_sales_stock_spends_combined\`
  WHERE brand_name = 'Lifelong'
  AND date BETWEEN '${fromDate}' AND '${toDate}'
  LIMIT ${limit}
  OFFSET ${offset}
`;

  try {
    const totalCount = await bigqueryClient.query({
      query: `SELECT COUNT(*) as total FROM \`hopeful-history-405018.Quickcommerce_final.product_sales_stock_spends_combined\` WHERE brand_name = 'Lifelong'`,
    });
    console.log(totalCount[0][0].total);
    const [rows] = await bigqueryClient.query({ query });
    // const filteredMetrics = await filterMetrics(rows, metric);
    // const sum = await sumMetrics(filteredMetrics);
    // const groupedData = await groupBySubcategory(filteredMetrics);
    // console.log("Query Results:", filteredMetrics);
    // console.log(rows);
    // const subcategory = await groupBySubcategory(rows);
    // console.log(filterMetrics);
    const summaryData = await summary(rows);

    res.status(200).json({
      success: true,
      data: rows,
      summary: summaryData,
      pagination: {
        total: totalCount[0][0].total,
        offset: offset,
        limit: limit,
        currentPage: Math.floor(offset / limit) + 1,
        totalPages: Math.ceil(totalCount[0][0].total / limit),
      },
    });
  } catch (error) {
    console.error("Error running query:", error);
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post("/datewisesCategory", async (req, res) => {
  try {
    let { from, to, metric, offset = 0, limit = 10 } = req.body;

    console.log(req.body);

    // Handle missing values
    if (!from || !to) {
      return res
        .status(400)
        .json({ success: false, message: "Missing 'from' or 'to' date" });
    }

    console.log(from, to, metric);

    const fromDate = await formatDate(from);
    const toDate = await formatDate(to);
    console.log(fromDate, toDate);

    // Get total count for pagination
    const totalCount = await bigqueryClient.query({
      query: `
        SELECT COUNT(*) as total 
        FROM \`hopeful-history-405018.Quickcommerce_final.product_sales_stock_spends_combined\` 
        WHERE brand_name = 'Lifelong'
        AND date BETWEEN '${fromDate}' AND '${toDate}'
      `,
    });

    const query = `
      SELECT * 
      FROM \`hopeful-history-405018.Quickcommerce_final.product_sales_stock_spends_combined\`
      WHERE brand_name = 'Lifelong'
      AND date BETWEEN '${fromDate}' AND '${toDate}'
      LIMIT ${limit}
      OFFSET ${offset}
    `;

    const [rows] = await bigqueryClient.query({ query });
    const categoryGrouping = await groupByCategory(rows);
    const summaryData = await summary(categoryGrouping);

    res.status(200).json({
      success: true,
      data: categoryGrouping,
      summary: summaryData,
      pagination: {
        total: totalCount[0][0].total,
        offset: offset,
        limit: limit,
        currentPage: Math.floor(offset / limit) + 1,
        totalPages: Math.ceil(totalCount[0][0].total / limit),
      },
    });
  } catch (error) {
    console.error("Error running query:", error);
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post("/datewisesSubCategory", async (req, res) => {
  try {
    let { from, to, metric, offset = 0, limit = 10 } = req.body;

    console.log(req.body);

    // Handle missing values
    if (!from || !to) {
      return res
        .status(400)
        .json({ success: false, message: "Missing 'from' or 'to' date" });
    }

    console.log(from, to, metric);

    const fromDate = await formatDate(from);
    const toDate = await formatDate(to);
    console.log(fromDate, toDate);

    // Get total count for pagination
    const totalCount = await bigqueryClient.query({
      query: `
        SELECT COUNT(*) as total 
        FROM \`hopeful-history-405018.Quickcommerce_final.product_sales_stock_spends_combined\` 
        WHERE brand_name = 'Lifelong'
        AND date BETWEEN '${fromDate}' AND '${toDate}'
      `,
    });

    const query = `
      SELECT * 
      FROM \`hopeful-history-405018.Quickcommerce_final.product_sales_stock_spends_combined\`
      WHERE brand_name = 'Lifelong'
      AND date BETWEEN '${fromDate}' AND '${toDate}'
      LIMIT ${limit}
      OFFSET ${offset}
    `;

    const [rows] = await bigqueryClient.query({ query });
    const categoryGrouping = await groupBySubcategory(rows);
    const summaryData = await summary(categoryGrouping);

    res.status(200).json({
      success: true,
      data: categoryGrouping,
      summary: summaryData,
      pagination: {
        total: totalCount[0][0].total,
        offset: offset,
        limit: limit,
        currentPage: Math.floor(offset / limit) + 1,
        totalPages: Math.ceil(totalCount[0][0].total / limit),
      },
    });
  } catch (error) {
    console.error("Error running query:", error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// app.listen(6060, () => console.log(`Server running on port 6060`));
module.exports = app;

// Run locally
// if (require.main === module) {
//   app.listen(6060, () => console.log(`Server running on http://localhost:6060`));
// }