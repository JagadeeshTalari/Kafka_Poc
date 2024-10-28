// grcWorkerApi.js

const express = require("express");
const mongoose = require("mongoose");
const { Kafka } = require("kafkajs");
const winston = require("winston");
const GrcRequest = require("./models/grcWorkerModel");

const app = express();
const PORT = 3004;

app.use(express.json());

// Connect to MongoDB
mongoose.connect("mongodb://localhost:27017/grcRequestsDB", {
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
  transports: [new winston.transports.File({ filename: "grcWorker.log" })],
});

// Kafka setup
const kafka = new Kafka({
  clientId: "grc-worker",
  brokers: ["localhost:9092"],
});
const consumer = kafka.consumer({ groupId: "grc-group" });
const producer = kafka.producer();

const run = async () => {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: "requests-topic", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const request = JSON.parse(message.value.toString());
      logger.info("Processing request from Requests API", { request });

      try {
        // Simulate processing the request
        const grcResponse = {
          id: request._id,
          status: "Processed",
          details: request.description,
        };

        // Save the GRC request to MongoDB (optional, for tracking)
        await GrcRequest.create(grcResponse);

        // Publish the response to the requests-topic
        await producer.send({
          topic: "grc-worker-topic",
          messages: [{ value: JSON.stringify(grcResponse) }],
        });

        // Log the successful processing
        logger.info("GRC request processed successfully", { grcResponse });
      } catch (error) {
        logger.error("Error processing GRC request", error);

        // Publish the error to the error-backup-topic
        await producer.send({
          topic: "error-backup-topic",
          messages: [
            {
              value: JSON.stringify({
                error: "Processing failed",
                details: request,
              }),
            },
          ],
        });
      }
    },
  });

  console.log("GRC Worker API connected to Kafka");
};

run().catch(console.error);

// CRUD operations

// Create a GRC Request
app.post("/grc-requests", async (req, res) => {
  try {
    const newGrcRequest = await GrcRequest.create(req.body);
    res.status(201).json(newGrcRequest);
  } catch (error) {
    logger.error("Error creating GRC request", error);
    res.status(500).json({ error: "Failed to create GRC request" });
  }
});

// Read all GRC Requests
app.get("/grc-requests", async (req, res) => {
  try {
    const grcRequests = await GrcRequest.find();
    res.status(200).json(grcRequests);
  } catch (error) {
    logger.error("Error fetching GRC requests", error);
    res.status(500).json({ error: "Failed to fetch GRC requests" });
  }
});

// Update a GRC Request
app.put("/grc-requests/:id", async (req, res) => {
  try {
    const updatedGrcRequest = await GrcRequest.findByIdAndUpdate(
      req.params.id,
      req.body,
      { new: true }
    );
    if (!updatedGrcRequest) {
      return res.status(404).json({ error: "GRC request not found" });
    }
    res.status(200).json(updatedGrcRequest);
  } catch (error) {
    logger.error("Error updating GRC request", error);
    res.status(500).json({ error: "Failed to update GRC request" });
  }
});

// Delete a GRC Request
app.delete("/grc-requests/:id", async (req, res) => {
  try {
    const deletedGrcRequest = await GrcRequest.findByIdAndDelete(req.params.id);
    if (!deletedGrcRequest) {
      return res.status(404).json({ error: "GRC request not found" });
    }
    res.status(204).send();
  } catch (error) {
    logger.error("Error deleting GRC request", error);
    res.status(500).json({ error: "Failed to delete GRC request" });
  }
});

app.listen(PORT, () => {
  console.log(`GRC Worker API running on http://localhost:${PORT}`);
});
