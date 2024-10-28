// grcWorkerApi.js

const express = require("express");
const mongoose = require("mongoose");
const { Kafka } = require("kafkajs");

const app = express();
const PORT = 3004;

app.use(express.json());

// Connect to MongoDB
mongoose.connect("mongodb://localhost:27017/grcTasksDB", {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});

// Kafka setup
const kafka = new Kafka({
  clientId: "grc-worker-api",
  brokers: ["localhost:9092"],
});
const producer = kafka.producer();

const runProducer = async () => {
  await producer.connect();
};
runProducer().catch(console.error);

// Define Task Schema and Model
const taskSchema = new mongoose.Schema(
  {
    requestId: String,
    data: String,
  },
  { timestamps: true }
);

const Task = mongoose.model("Task", taskSchema);

// Create a new task
app.post("/tasks", async (req, res) => {
  try {
    const newTask = new Task(req.body);
    const savedTask = await newTask.save();

    // Send a message to Kafka
    await producer.send({
      topic: "task-created",
      messages: [{ value: JSON.stringify(savedTask) }],
    });

    res.json({ message: "Task created", task: savedTask });
  } catch (error) {
    console.error("Error creating task:", error.message);
    res
      .status(500)
      .json({ error: "Failed to create task", details: error.message });
  }
});

// Additional CRUD operations with error handling...

app.listen(PORT, () => {
  console.log(`GRC Worker API running on http://localhost:${PORT}`);
});
