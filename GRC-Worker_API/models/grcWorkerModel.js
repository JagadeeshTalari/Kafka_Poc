// models/grcWorkerModel.js

const mongoose = require("mongoose");

const grcWorkerSchema = new mongoose.Schema({
  requestId: { type: String, required: true },
  response: { type: String },
  status: { type: String, default: "processed" },
  createdAt: { type: Date, default: Date.now },
});

module.exports = mongoose.model("GrcWorker", grcWorkerSchema);
