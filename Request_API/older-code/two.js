// requestsApi.js

const express = require("express");
const mongoose = require("mongoose");
const axios = require("axios");
const { Kafka } = require("kafkajs");

const app = express();
const PORT = 3001;

app.use(express.json());

// Connect to MongoDB
mongoose.connect("mongodb://localhost:27017/requestsDB", {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});

// Kafka setup
const kafka = new Kafka({
  clientId: "requests-api",
  brokers: ["localhost:9092"],
});
const producer = kafka.producer();

const runProducer = async () => {
  await producer.connect();
};
runProducer().catch(console.error);

// Define Request Schema and Model
const requestSchema = new mongoose.Schema(
  {
    data: String,
  },
  { timestamps: true }
);

const Request = mongoose.model("Request", requestSchema);

// Other API URLs
const NOTIFICATION_API_URL = "http://localhost:3002";
const AUDIT_LOGGER_URL = "http://localhost:3003";
const GRC_WORKER_URL = "http://localhost:3004";

// Create a new request
app.post("/requests", async (req, res) => {
  try {
    const newRequest = new Request(req.body);
    const savedRequest = await newRequest.save();

    // Send a message to Kafka
    await producer.send({
      topic: "request-created",
      messages: [{ value: JSON.stringify(savedRequest) }],
    });

    // Send to GRC Worker API
    const grcResponse = await axios.post(`${GRC_WORKER_URL}/tasks`, {
      requestId: savedRequest._id,
      data: savedRequest.data,
    });

    // Notify and log
    await axios.post(`${NOTIFICATION_API_URL}/notify`, {
      message: "New request created",
      details: grcResponse.data,
    });
    await axios.post(`${AUDIT_LOGGER_URL}/log`, {
      action: "Create Request",
      status: "Success",
      timestamp: new Date().toISOString(),
    });

    res.json({
      message: "Request created",
      request: savedRequest,
      grcResult: grcResponse.data,
    });
  } catch (error) {
    console.error("Error creating request:", error.message);
    res
      .status(500)
      .json({ error: "Failed to create request", details: error.message });
  }
});

// Additional CRUD operations with error handling...

app.listen(PORT, () => {
  console.log(`Requests API running on http://localhost:${PORT}`);
});
