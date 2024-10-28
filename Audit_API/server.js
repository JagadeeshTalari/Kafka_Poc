// auditApi.js

const express = require("express");
const mongoose = require("mongoose");
const { Kafka } = require("kafkajs");
const winston = require("winston");
const AuditLog = require("./models/auditLogModel");

const app = express();
const PORT = 3003;

app.use(express.json());

// Connect to MongoDB
mongoose.connect("mongodb://localhost:27017/auditLogsDB", {
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
  transports: [new winston.transports.File({ filename: "audit.log" })],
});

// Kafka setup
const kafka = new Kafka({ clientId: "audit-api", brokers: ["localhost:9092"] });
const consumer = kafka.consumer({ groupId: "audit-group" });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "requests-topic", fromBeginning: true });
  await consumer.subscribe({ topic: "grc-worker-topic", fromBeginning: true });
  await consumer.subscribe({
    topic: "error-backup-topic",
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const logData = JSON.parse(message.value.toString());
      logger.info(`Event received from ${topic}`, { logData });

      // Store audit log in MongoDB
      await AuditLog.create({ action: topic, details: logData });
    },
  });
};

run().catch(console.error);

app.listen(PORT, () => {
  console.log(`Audit API running on http://localhost:${PORT}`);
});
