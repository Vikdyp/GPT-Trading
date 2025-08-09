"use client";
import React, { useEffect, useState } from "react";
const API = process.env.NEXT_PUBLIC_API_BASE || "http://localhost:8000";

export default function Positions() {
  const [positions, setPositions] = useState<any[]>([]);
  const [tp, setTp] = useState(2);
  const [sl, setSl] = useState(1);

  const refresh = async () => {
    const r = await fetch(API + "/positions");
    const j = await r.json();
    setPositions(j.positions || []);
  };

  useEffect(() => {
    refresh();
    const t = setInterval(refresh, 4000);
    return () => clearInterval(t);
  }, []);

  const closePct = async (symbol: string, pct: number) => {
    await fetch(`${API}/positions/${symbol}/close`, {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({ percent: pct })
    });
    refresh();
  };

  const placeOco = async (symbol: string, pct: number) => {
    await fetch(`${API}/positions/${symbol}/oco`, {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({
        percent_of_position: pct,
        tp_pct: Number(tp),
        sl_pct: Number(sl),
        basis: "avg"
      })
    });
  };

  return (
    <div>
      <h2>Positions</h2>
      <div style={{ marginBottom: 12 }}>
        <label>TP %:&nbsp;</label>
        <input type="number" value={tp} onChange={e=>setTp(Number(e.target.value))} style={{ width: 80 }}/>
        <label style={{marginLeft:12}}>SL %:&nbsp;</label>
        <input type="number" value={sl} onChange={e=>setSl(Number(e.target.value))} style={{ width: 80 }}/>
      </div>
      <table cellPadding={8} style={{ borderCollapse: "collapse", width: "100%" }}>
        <thead><tr>
          <th>Symbole</th><th>Qty</th><th>AvgPx</th><th>Mark</th><th>Unreal €</th><th>Unreal %</th><th>MAJ</th><th>Actions</th>
        </tr></thead>
        <tbody>
          {positions.map((p:any) => (
            <tr key={p.symbol} style={{ borderTop: "1px solid #eee" }}>
              <td>{p.symbol}</td>
              <td>{Number(p.qty).toFixed(6)}</td>
              <td>{p.avg_px ? Number(p.avg_px).toFixed(6) : "-"}</td>
              <td>{p.mark ? Number(p.mark).toFixed(6) : "-"}</td>
              <td style={{color:(p.unreal_pnl||0)<0?"#b00":"#0a0"}}>
                {p.unreal_pnl ? p.unreal_pnl.toFixed(2) : "0.00"}
              </td>
              <td>{p.unreal_pnl_pct ? p.unreal_pnl_pct.toFixed(2)+"%" : "-"}</td>
              <td>{new Date(p.updated_at).toLocaleString()}</td>
              <td>
                <button onClick={()=>closePct(p.symbol,25)}>Close 25%</button>&nbsp;
                <button onClick={()=>closePct(p.symbol,50)}>Close 50%</button>&nbsp;
                <button onClick={()=>closePct(p.symbol,100)}>Close 100%</button>&nbsp;
                <button onClick={()=>placeOco(p.symbol,100)} title="Place un OCO sur 100% de la position">Set OCO</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <p style={{color:"#666"}}>En mode paper, OCO est simulé (ordre non envoyé). En mode live, un vrai OCO est placé.</p>
    </div>
  );
}
