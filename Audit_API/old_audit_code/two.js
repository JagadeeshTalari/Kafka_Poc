// auditLoggerApi.js

const express = require("express");
const mongoose = require("mongoose");
const { Kafka } = require("kafkajs");

const app = express();
const PORT = 3003;

app.use(express.json());

// Connect to MongoDB
mongoose.connect("mongodb://localhost:27017/auditLogsDB", {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});

// Kafka setup
const kafka = new Kafka({
  clientId: "audit-logger-api",
  brokers: ["localhost:9092"],
});
const consumer = kafka.consumer({ groupId: "audit-log-group" });

const runConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "request-created", fromBeginning: true });
  await consumer.subscribe({ topic: "request-updated", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const logData = JSON.parse(message.value.toString());
      // Save the log to MongoDB or perform additional logging
      console.log("New log event:", logData);
    },
  });
};
runConsumer().catch(console.error);

// Define Audit Log Schema and Model
const auditLogSchema = new mongoose.Schema(
  {
    action: String,
    status: String,
    timestamp: { type: Date, default: Date.now },
  },
  { timestamps: true }
);

const AuditLog = mongoose.model("AuditLog", auditLogSchema);

// Create a new log entry
app.post("/log", async (req, res) => {
  try {
    const newLog = new AuditLog(req.body);
    const savedLog = await newLog.save();
    res.json({ message: "Log entry created", log: savedLog });
  } catch (error) {
    console.error("Error creating log entry:", error.message);
    res
      .status(500)
      .json({ error: "Failed to create log entry", details: error.message });
  }
});

// Additional CRUD operations with error handling...

app.listen(PORT, () => {
  console.log(`Audit Logger API running on http://localhost:${PORT}`);
});
