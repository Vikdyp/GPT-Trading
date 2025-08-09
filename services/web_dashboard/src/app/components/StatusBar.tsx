"use client";
import React from "react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://localhost:8000";

export default function StatusBar(){
  const [st, setSt] = React.useState<any>(null);

  const refresh = async ()=> {
    try{
      const r = await fetch(`${API}/bot/status`, { cache: "no-store" });
      const j = await r.json();
      setSt(j);
    }catch{}
  };

  React.useEffect(()=>{
    refresh();
    const t = setInterval(refresh, 4000);
    return ()=> clearInterval(t);
  },[]);

  const toggle = async (on:boolean)=>{
    await fetch(`${API}${on?"/bot/enable":"/bot/disable"}`, { method:"POST" });
    refresh();
  };

  const badge = st?.kill_active ? {bg:"#b00", text:"KILL-SWITCH"} : {bg:"#0a0", text:"OK"};
  const rel = st?.config_last_reload ? new Date(st.config_last_reload).toLocaleTimeString() : "-";
  const fmt = (x:any)=> (typeof x === "number" ? x.toFixed(2) : "-");

  return (
    <div style={{display:"flex", gap:12, alignItems:"center", width:"100%", color:"#fff"}}>
      <span style={{background:badge.bg, padding:"2px 6px", borderRadius:4}}>{badge.text}</span>
      <span>Mode: {st?.mode ?? "-"}</span>
      <span>Bot: {st?.enabled ? "ON" : "OFF"}</span>
      <span>PnL latent: {fmt(st?.unreal_pnl)} €</span>
      <span>PnL jour: {fmt(st?.daily_pnl_eur)} €</span>
      <span>Seuil jour: {fmt(st?.daily_limit_eur)} €</span>
      <span>Equity: {fmt(st?.equity_eur)} €</span>
      <span>Config: maj {rel}</span>
      <div style={{marginLeft:"auto"}}>
        <button onClick={()=>toggle(true)} disabled={!!st?.enabled}>Enable</button>&nbsp;
        <button onClick={()=>toggle(false)} disabled={!st?.enabled}>Disable</button>
      </div>
    </div>
  );
}
