// k6 load: an open model at a fixed request rate (requests keep arriving even if the server slows down).
// 70% GET /items/{id}, 30% POST /orders.   k6 run -e RATE=250 -e DURATION=30s -e BASE_URL=http://127.0.0.1:8900 load.js
import http from "k6/http";

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ["avg", "p(50)", "p(95)", "p(99)", "max"],
  scenarios: {
    load: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.RATE || 100),
      timeUnit: "1s",
      duration: __ENV.DURATION || "30s",
      preAllocatedVUs: 50,
      maxVUs: 400,
    },
  },
};

const BASE = __ENV.BASE_URL || "http://127.0.0.1:8900";
const params = { headers: { "Content-Type": "application/json" } };

export default function () {
  const id = 1 + Math.floor(Math.random() * 1000);
  if (Math.random() < 0.7) {
    http.get(`${BASE}/items/${id}`);
  } else {
    http.post(`${BASE}/orders`, JSON.stringify({ item_id: id, qty: 1 }), params);
  }
}
