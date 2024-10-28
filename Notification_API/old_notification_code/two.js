// notificationApi.js

const express = require("express");
const mongoose = require("mongoose");
const { Kafka } = require("kafkajs");

const app = express();
const PORT = 3002;

app.use(express.json());

// Connect to MongoDB
mongoose.connect("mongodb://localhost:27017/notificationsDB", {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});

// Kafka setup
const kafka = new Kafka({
  clientId: "notification-api",
  brokers: ["localhost:9092"],
});
const consumer = kafka.consumer({ groupId: "notification-group" });

const runConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "request-created", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const notificationData = JSON.parse(message.value.toString());
      // Log the notification or save it to the database
      console.log("New notification:", notificationData);
    },
  });
};
runConsumer().catch(console.error);

// Define Notification Schema and Model
const notificationSchema = new mongoose.Schema(
  {
    message: String,
    details: Object,
  },
  { timestamps: true }
);

const Notification = mongoose.model("Notification", notificationSchema);

// Create a new notification
app.post("/notify", async (req, res) => {
  try {
    const newNotification = new Notification(req.body);
    const savedNotification = await newNotification.save();
    res.json({
      message: "Notification created",
      notification: savedNotification,
    });
  } catch (error) {
    console.error("Error creating notification:", error.message);
    res
      .status(500)
      .json({ error: "Failed to create notification", details: error.message });
  }
});

// Additional CRUD operations with error handling...

app.listen(PORT, () => {
  console.log(`Notification API running on http://localhost:${PORT}`);
});
