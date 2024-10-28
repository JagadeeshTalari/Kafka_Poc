// requestsApi.js

const express = require("express");
const mongoose = require("mongoose");
const { Kafka } = require("kafkajs");
const winston = require("winston");
const Request = require("./models/requestModel");

const app = express();
const PORT = 3001;

app.use(express.json());

// Connect to MongoDB
mongoose.connect("mongodb://localhost:27017/requestsDB", {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});

// Configure winston logger
const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [new winston.transports.File({ filename: "requests.log" })],
});

// Kafka setup
const kafka = new Kafka({
  clientId: "requests-api",
  brokers: ["localhost:9092"],
});
const producer = kafka.producer();

const run = async () => {
  await producer.connect();
};

run().catch(console.error);

// CRUD Operations

// Create a new request
app.post("/requests", async (req, res) => {
  const requestData = req.body;

  try {
    const newRequest = await Request.create(requestData);
    await producer.send({
      topic: "requests-topic",
      messages: [{ value: JSON.stringify(newRequest) }],
    });
    logger.info("New request published", { requestData: newRequest });
    res.status(201).json({ message: "Request created", data: newRequest });
  } catch (error) {
    logger.error("Error creating request", { error: error.message });
    res
      .status(500)
      .json({ error: "Failed to create request", details: error.message });
  }
});

// Get all requests
app.get("/requests", async (req, res) => {
  try {
    const requests = await Request.find();
    res.status(200).json(requests);
  } catch (error) {
    logger.error("Error fetching requests", { error: error.message });
    res
      .status(500)
      .json({ error: "Failed to fetch requests", details: error.message });
  }
});

// Update a request
app.put("/requests/:id", async (req, res) => {
  const { id } = req.params;
  const requestData = req.body;

  try {
    const updatedRequest = await Request.findByIdAndUpdate(id, requestData, {
      new: true,
    });
    if (!updatedRequest)
      return res.status(404).json({ error: "Request not found" });

    await producer.send({
      topic: "requests-topic",
      messages: [{ value: JSON.stringify(updatedRequest) }],
    });
    logger.info("Request updated", { requestData: updatedRequest });
    res.status(200).json({ message: "Request updated", data: updatedRequest });
  } catch (error) {
    logger.error("Error updating request", { error: error.message });
    res
      .status(500)
      .json({ error: "Failed to update request", details: error.message });
  }
});

// Delete a request
app.delete("/requests/:id", async (req, res) => {
  const { id } = req.params;

  try {
    const deletedRequest = await Request.findByIdAndDelete(id);
    if (!deletedRequest)
      return res.status(404).json({ error: "Request not found" });

    logger.info("Request deleted", { id });
    res.status(200).json({ message: "Request deleted", data: deletedRequest });
  } catch (error) {
    logger.error("Error deleting request", { error: error.message });
    res
      .status(500)
      .json({ error: "Failed to delete request", details: error.message });
  }
});

app.listen(PORT, () => {
  console.log(`Requests API running on http://localhost:${PORT}`);
});
