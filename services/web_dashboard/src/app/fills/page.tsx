"use client";
import React, { useEffect, useState } from "react";
const API = process.env.NEXT_PUBLIC_API_BASE || "http://localhost:8000";

export default function Fills() {
  const [fills, setFills] = useState<any[]>([]);
  useEffect(() => {
    const fetchFills = async () => {
      const r = await fetch(API + "/fills?limit=200", { cache: "no-store" });
      const j = await r.json();
      setFills(j.fills || []);
    };
    fetchFills();
    const t = setInterval(fetchFills, 4000);
    return () => clearInterval(t);
  }, []);

  return (
    <div>
      <h2>Fills (exécutions simulées)</h2>
      <table cellPadding={8} style={{ borderCollapse:"collapse", width:"100%" }}>
        <thead><tr>
          <th>ID</th><th>OrderID</th><th>Prix</th><th>Qty</th><th>Fee</th><th>TS</th>
        </tr></thead>
        <tbody>
          {fills.map((f:any) => (
            <tr key={f.id} style={{ borderTop:"1px solid #eee" }}>
              <td>{f.id}</td>
              <td>{f.order_id ?? "-"}</td>
              <td>{Number(f.price).toFixed(6)}</td>
              <td style={{color: (f.qty||0)<0 ? "#b00" : "#0a0"}}>{Number(f.qty).toFixed(6)}</td>
              <td>{f.fee ? Number(f.fee).toFixed(6) : "-"}</td>
              <td>{new Date(f.ts).toLocaleString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p style={{color:"#666"}}>En mode paper, ces fills proviennent des simulations effectuées après un order.test ou trailing.</p>
    </div>
  );
}
