// requestsApi.js

const express = require("express");
const axios = require("axios");
const app = express();
const PORT = 3001;

app.use(express.json());

const NOTIFICATION_API_URL = "http://localhost:3002";
const AUDIT_LOGGER_URL = "http://localhost:3003";
const GRC_WORKER_URL = "http://localhost:3004";

let requests = []; // In-memory data storage

// Create a new request
app.post("/requests", async (req, res) => {
  try {
    const newRequest = { id: Date.now(), ...req.body };
    requests.push(newRequest);

    // Send task to GRC Worker API
    const grcResponse = await axios.post(`${GRC_WORKER_URL}/tasks`, {
      requestId: newRequest.id,
      data: newRequest.data,
    });

    // Notify other services
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
      request: newRequest,
      grcResult: grcResponse.data,
    });
  } catch (error) {
    console.error("Error creating request:", error.message);
    res
      .status(500)
      .json({ error: "Failed to create request", details: error.message });
  }
});

// Read all requests
app.get("/requests", (req, res) => {
  try {
    res.json(requests);
  } catch (error) {
    console.error("Error fetching requests:", error.message);
    res
      .status(500)
      .json({ error: "Failed to fetch requests", details: error.message });
  }
});

// Read a specific request by ID
app.get("/requests/:id", (req, res) => {
  try {
    const request = requests.find((r) => r.id === Number(req.params.id));
    if (request) {
      res.json(request);
    } else {
      res.status(404).json({ message: "Request not found" });
    }
  } catch (error) {
    console.error("Error fetching request:", error.message);
    res
      .status(500)
      .json({ error: "Failed to fetch request", details: error.message });
  }
});

// Update a request
app.put("/requests/:id", async (req, res) => {
  try {
    const index = requests.findIndex((r) => r.id === Number(req.params.id));
    if (index !== -1) {
      requests[index] = { ...requests[index], ...req.body };

      // Update task in GRC Worker API
      await axios.put(`${GRC_WORKER_URL}/tasks/${requests[index].id}`, {
        data: requests[index].data,
      });

      await axios.post(`${AUDIT_LOGGER_URL}/log`, {
        action: "Update Request",
        status: "Success",
        timestamp: new Date().toISOString(),
      });

      res.json({ message: "Request updated", request: requests[index] });
    } else {
      res.status(404).json({ message: "Request not found" });
    }
  } catch (error) {
    console.error("Error updating request:", error.message);
    res
      .status(500)
      .json({ error: "Failed to update request", details: error.message });
  }
});

// Delete a request
app.delete("/requests/:id", async (req, res) => {
  try {
    const index = requests.findIndex((r) => r.id === Number(req.params.id));
    if (index !== -1) {
      const deletedRequest = requests.splice(index, 1)[0];

      // Delete task in GRC Worker API
      await axios.delete(`${GRC_WORKER_URL}/tasks/${deletedRequest.id}`);

      await axios.post(`${NOTIFICATION_API_URL}/notify`, {
        message: "Request deleted",
        details: deletedRequest,
      });
      await axios.post(`${AUDIT_LOGGER_URL}/log`, {
        action: "Delete Request",
        status: "Success",
        timestamp: new Date().toISOString(),
      });

      res.json({ message: "Request deleted", request: deletedRequest });
    } else {
      res.status(404).json({ message: "Request not found" });
    }
  } catch (error) {
    console.error("Error deleting request:", error.message);
    res
      .status(500)
      .json({ error: "Failed to delete request", details: error.message });
  }
});

app.listen(PORT, () => {
  console.log(`Requests API running on http://localhost:${PORT}`);
});
