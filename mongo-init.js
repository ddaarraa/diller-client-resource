db = connect("mongodb://localhost:27017/admin");

const config = {
  _id: "rs0",
  members: [{ _id: 0, host: "localhost:27017" }]
};

try {
  rs.initiate(config);
  print("✅ Replica set initialized successfully!");
} catch (e) {
  print("⚠️ Replica set already initialized or failed: " + e);
}