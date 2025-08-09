"use client";
import React, { useEffect, useState } from "react";
const API = process.env.NEXT_PUBLIC_API_BASE || "http://localhost:8000";

type StratValues = {
  ema_fast: number; ema_slow: number;
  rsi_len: number; rsi_buy_max: number;
  main_tf: string;
  atr_len: number;
  atr_mult_stop: number;
  atr_min_stop_pct: number;
  reward_risk: number;
  trend_tf: string;
  ema_trend_len: number;
  cooldown_s: number;

  // --- nouveaux champs ---
  auto_oco_enabled: boolean;
  trailing_enabled: boolean;
  atr_trail_mult: number;
  oco_percent: number; // 0..100
};

export default function StrategyPage() {
  const [loading, setLoading] = useState(true);
  const [values, setValues] = useState<StratValues>({
    ema_fast: 9, ema_slow: 21,
    rsi_len: 14, rsi_buy_max: 70,
    main_tf: "1m",
    atr_len: 14,
    atr_mult_stop: 1.8,
    atr_min_stop_pct: 0.003, // 0.3%
    reward_risk: 2.0,
    trend_tf: "4h",
    ema_trend_len: 50,
    cooldown_s: 45,
    // nouveaux défauts
    auto_oco_enabled: true,
    trailing_enabled: true,
    atr_trail_mult: 1.2,
    oco_percent: 100,
  });

  const load = async () => {
    setLoading(true);
    const eff = (await (await fetch(API + "/config/effective", { cache: "no-store" })).json()).config;
    const ov  = (await (await fetch(API + "/config/overrides", { cache: "no-store" })).json()).overrides || {};
    const s = { ...(eff?.strategy || {}), ...(ov?.strategy || {}) };

    setValues({
      ema_fast: s.ema_fast ?? 9,
      ema_slow: s.ema_slow ?? 21,
      rsi_len: s.rsi_len ?? 14,
      rsi_buy_max: s.rsi_buy_max ?? 70,
      main_tf: s.main_tf ?? "1m",
      atr_len: s.atr_len ?? 14,
      atr_mult_stop: s.atr_mult_stop ?? 1.8,
      atr_min_stop_pct: s.atr_min_stop_pct ?? 0.003,
      reward_risk: s.reward_risk ?? 2.0,
      trend_tf: s.trend_tf ?? "4h",
      ema_trend_len: s.ema_trend_len ?? 50,
      cooldown_s: s.cooldown_s ?? 45,

      auto_oco_enabled: s.auto_oco_enabled ?? true,
      trailing_enabled: s.trailing_enabled ?? true,
      atr_trail_mult: s.atr_trail_mult ?? 1.2,
      oco_percent: s.oco_percent ?? 100,
    });
    setLoading(false);
  };

  useEffect(() => { load(); }, []);

  const save = async () => {
    const body = { strategy: {
      ema_fast: Number(values.ema_fast),
      ema_slow: Number(values.ema_slow),
      rsi_len: Number(values.rsi_len),
      rsi_buy_max: Number(values.rsi_buy_max),
      main_tf: values.main_tf,
      atr_len: Number(values.atr_len),
      atr_mult_stop: Number(values.atr_mult_stop),
      atr_min_stop_pct: Number(values.atr_min_stop_pct),
      reward_risk: Number(values.reward_risk),
      trend_tf: values.trend_tf,
      ema_trend_len: Number(values.ema_trend_len),
      cooldown_s: Number(values.cooldown_s),

      auto_oco_enabled: !!values.auto_oco_enabled,
      trailing_enabled: !!values.trailing_enabled,
      atr_trail_mult: Number(values.atr_trail_mult),
      oco_percent: Number(values.oco_percent),
    }};
    await fetch(API + "/config/overrides", {
      method: "PUT", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    alert("Paramètres stratégie sauvegardés (hot-reload actif).");
  };

  const set = (k: keyof StratValues, v: any) =>
    setValues((x) => ({ ...x, [k]: v }));

  if (loading) return <div>Chargement…</div>;

  return (
    <div style={{ padding: 16 }}>
      <h2>Stratégie — paramètres</h2>

      <h3 style={{ marginTop: 12 }}>Signaux & Filtres</h3>
      <div style={{display:"grid", gridTemplateColumns:"240px 240px", gap:8, alignItems:"center", maxWidth:720}}>
        <label>EMA fast</label>
        <input type="number" value={values.ema_fast} onChange={e=>set("ema_fast", Number(e.target.value))}/>
        <label>EMA slow</label>
        <input type="number" value={values.ema_slow} onChange={e=>set("ema_slow", Number(e.target.value))}/>
        <label>RSI length</label>
        <input type="number" value={values.rsi_len} onChange={e=>set("rsi_len", Number(e.target.value))}/>
        <label>RSI max pour BUY</label>
        <input type="number" value={values.rsi_buy_max} onChange={e=>set("rsi_buy_max", Number(e.target.value))}/>
        <label>TF principal (ATR)</label>
        <input value={values.main_tf} onChange={e=>set("main_tf", e.target.value)}/>
        <label>ATR length</label>
        <input type="number" value={values.atr_len} onChange={e=>set("atr_len", Number(e.target.value))}/>
        <label>ATR × stop multiplier</label>
        <input type="number" step="0.1" value={values.atr_mult_stop} onChange={e=>set("atr_mult_stop", Number(e.target.value))}/>
        <label>Stop min (fraction)</label>
        <input type="number" step="0.001" value={values.atr_min_stop_pct} onChange={e=>set("atr_min_stop_pct", Number(e.target.value))}/>
        <label>Reward / Risk</label>
        <input type="number" step="0.1" value={values.reward_risk} onChange={e=>set("reward_risk", Number(e.target.value))}/>
        <label>TF de tendance (HTF)</label>
        <input value={values.trend_tf} onChange={e=>set("trend_tf", e.target.value)}/>
        <label>EMA trend length</label>
        <input type="number" value={values.ema_trend_len} onChange={e=>set("ema_trend_len", Number(e.target.value))}/>
        <label>Cooldown entre signaux (s)</label>
        <input type="number" value={values.cooldown_s} onChange={e=>set("cooldown_s", Number(e.target.value))}/>
      </div>

      <h3 style={{ marginTop: 16 }}>Gestion position — Auto-OCO & Trailing</h3>
      <div style={{display:"grid", gridTemplateColumns:"240px 240px", gap:8, alignItems:"center", maxWidth:720}}>
        <label>Auto-OCO activé</label>
        <input type="checkbox"
               checked={!!values.auto_oco_enabled}
               onChange={e=>set("auto_oco_enabled", e.target.checked)} />

        <label>Trailing ATR activé</label>
        <input type="checkbox"
               checked={!!values.trailing_enabled}
               onChange={e=>set("trailing_enabled", e.target.checked)} />

        <label>ATR trail multiplier</label>
        <input type="number" step="0.1"
               value={values.atr_trail_mult}
               onChange={e=>set("atr_trail_mult", Number(e.target.value))} />

        <label>Couverture OCO (%)</label>
        <input type="number" step="1" min={0} max={100}
               value={values.oco_percent}
               onChange={e=>set("oco_percent", Number(e.target.value))} />
      </div>

      <div style={{ marginTop: 16 }}>
        <button onClick={save}>Enregistrer</button>
      </div>
      <p style={{ color: "#666", marginTop: 8 }}>
        Le bot applique ces paramètres en live (hot-reload). ATR et tendance sont calculés à la clôture des bougies.
      </p>
    </div>
  );
}
