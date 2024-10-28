// notificationsApi.js

const express = require("express");
const mongoose = require("mongoose");
const { Kafka } = require("kafkajs");
const winston = require("winston");
const Notification = require("./models/notificationModel");

const app = express();
const PORT = 3002;

app.use(express.json());

// Connect to MongoDB
mongoose.connect("mongodb://localhost:27017/notificationsDB", {
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
  transports: [new winston.transports.File({ filename: "notifications.log" })],
});

// Kafka setup
const kafka = new Kafka({
  clientId: "notifications-api",
  brokers: ["localhost:9092"],
});
const consumer = kafka.consumer({ groupId: "notification-group" });
const producer = kafka.producer();

const run = async () => {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({
    topic: "error-backup-topic",
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const notificationData = JSON.parse(message.value.toString());
      logger.info(`Notification processed from ${topic}`, { notificationData });

      // If there's an error, publish an alert
      await Notification.create({
        message: "Alert: An error occurred",
        details: notificationData,
      });
      logger.info("Alert notification created", { notificationData });
    },
  });
};

run().catch(console.error);

app.listen(PORT, () => {
  console.log(`Notifications API running on http://localhost:${PORT}`);
});
