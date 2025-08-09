"use client";
import React, { useEffect, useState } from "react";
const API = process.env.NEXT_PUBLIC_API_BASE || "http://localhost:8000";

export default function Orders() {
  const [orders, setOrders] = useState<any[]>([]);
  useEffect(() => {
    const fetchOrders = async () => {
      const r = await fetch(API + "/orders");
      const j = await r.json();
      setOrders(j.orders || []);
    };
    fetchOrders();
    const t = setInterval(fetchOrders, 4000);
    return () => clearInterval(t);
  }, []);

  return (
    <div>
      <h2>Ordres récents</h2>
      <table cellPadding={8} style={{ borderCollapse: "collapse", width: "100%" }}>
        <thead><tr>
          <th>ID</th><th>ClientID</th><th>Symbole</th><th>Side</th><th>Type</th>
          <th>Qty</th><th>Prix</th><th>Status</th><th>Créé</th>
        </tr></thead>
        <tbody>
          {orders.map(o => (
            <tr key={o.id} style={{ borderTop: "1px solid #eee" }}>
              <td>{o.id}</td>
              <td>{o.client_id}</td>
              <td>{o.symbol}</td>
              <td>{o.side}</td>
              <td>{o.type}</td>
              <td>{o.qty}</td>
              <td>{o.price ?? "-"}</td>
              <td>{o.status}</td>
              <td>{new Date(o.created_at).toLocaleString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p style={{color:"#666"}}>Les /order/test sont enregistrés pour le suivi.</p>
    </div>
  );
}
