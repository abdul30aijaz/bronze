import requests
from datetime import datetime

# Per-status colour tokens used across the email HTML
_STATUS_FG     = {"SUCCESS": "#155724", "FAILED": "#721c24", "PARTIAL": "#856404"}
_STATUS_BG     = {"SUCCESS": "#d4edda", "FAILED": "#f8d7da", "PARTIAL": "#fff3cd"}
_STATUS_ACCENT = {"SUCCESS": "#1a7a4a", "FAILED": "#c0392b", "PARTIAL": "#d97706"}
_STATUS_EMOJI  = {"SUCCESS": "✅",      "FAILED": "❌",       "PARTIAL": "⚠️"}


def _is_email(value):
    """Return True if value looks like a valid email address."""
    return isinstance(value, str) and "@" in value and "." in value.split("@")[-1]


def resolve_receivers(notify_config, log_fn=None, fallback_receivers=None):
    """Extract and validate email addresses from the notify config.

    Deduplicates addresses, skips invalid ones with a warning, and falls
    back to fallback_receivers (or returns []) if nothing valid is found.
    """
    resolved = []

    for addr in (notify_config or {}).get("emails", []):
        addr = (addr or "").strip()
        if not addr:
            continue
        if _is_email(addr):
            resolved.append(addr)
        else:
            msg = f"[notify] '{addr}' in notify.emails is not a valid email — skipping."
            (log_fn(msg) if log_fn else print(msg))

    resolved = list(dict.fromkeys(resolved))  # deduplicate, preserve order

    if not resolved:
        if fallback_receivers:
            msg = f"[notify] No valid receivers in notify config — using fallback: {fallback_receivers}"
            (log_fn(msg) if log_fn else print(msg))
            return fallback_receivers
        msg = f"[notify] No valid receivers resolved from notify block: {notify_config} — notification skipped."
        (log_fn(msg) if log_fn else print(msg))
        return []

    return resolved


def _overall_status(table_results):
    """Return SUCCESS / FAILED / PARTIAL based on the set of table statuses."""
    statuses = {r["status"] for r in table_results}
    if statuses == {"SUCCESS"}:
        return "SUCCESS"
    if statuses == {"FAILED"}:
        return "FAILED"
    return "PARTIAL"


def _overall_dq_pct(table_results):
    """Return the average DQ score across tables as a percentage, or None if unavailable."""
    scores = [r["dq_score"] for r in table_results if r.get("dq_score") is not None]
    return round(sum(scores) / len(scores) * 100, 1) if scores else None


def _format_dq(dq_pct):
    """Format a DQ percentage float as a string, or return 'N/A'."""
    return f"{dq_pct}%" if dq_pct is not None else "N/A"


def _render_table_rows(table_results):
    """Build HTML <tr> blocks for each table in the results summary section."""
    rows = []
    for r in table_results:
        fg        = _STATUS_FG.get(r["status"], "#333")
        rows_cell = f"{r['rows_ingested']:,}" if r.get("rows_ingested") is not None else "—"
        dq_cell   = f"{r['dq_score'] * 100:.1f}%" if r.get("dq_score") is not None else "—"

        # Inline failure detail shown below the status badge for failed tables
        failure_detail = ""
        if r["status"] == "FAILED":
            reason = r.get("failure_reason") or "Unknown error"
            msg    = r.get("dq_message") or ""
            failure_detail = f"""
                <div style="font-size:11px;color:#c0392b;margin-top:3px;line-height:1.4;">
                  &#x2717; {reason}
                  {"<br><span style='color:#777;font-style:italic;'>" + msg + "</span>" if msg else ""}
                </div>"""

        rows.append(f"""
        <tr style="border-bottom:1px solid #f0f0f0;">
          <td style="padding:10px 12px;font-family:monospace;font-size:12px;color:#333;">{r['table_name']}</td>
          <td style="padding:10px 12px;text-align:center;">
            <span style="font-weight:700;color:{fg};">{r['status']}</span>
            {failure_detail}
          </td>
          <td style="padding:10px 12px;text-align:right;font-size:13px;color:#555;">{rows_cell}</td>
          <td style="padding:10px 12px;text-align:center;font-size:13px;color:#555;">{dq_cell}</td>
        </tr>""")
    return "\n".join(rows)


def build_email_payload(pipeline_name, run_id, run_link, table_results, run_timestamp=None):
    """Assemble the structured payload dict consumed by render_email_html and _build_subject."""
    status  = _overall_status(table_results)
    dq_pct  = _overall_dq_pct(table_results)
    total   = len(table_results)
    success = sum(1 for r in table_results if r["status"] == "SUCCESS")
    return {
        "pipeline_name":    pipeline_name,
        "run_id":           run_id,
        "run_link":         run_link,
        "run_timestamp":    (run_timestamp or datetime.utcnow()).strftime("%Y-%m-%d %H:%M UTC"),
        "overall_status":   status,
        "overall_dq_pct":   dq_pct,
        "total_tables":     total,
        "succeeded_tables": success,
        "failed_tables":    total - success,
        "table_results":    table_results,
    }


def render_email_html(payload):
    """Render the full HTML email string from a build_email_payload dict.

    DQ colour thresholds: green ≥ 90%, amber ≥ 70%, red below 70%.
    The 'View Pipeline Run' button is omitted if run_link is not set.
    """
    status     = payload["overall_status"]
    accent     = _STATUS_ACCENT[status]
    banner_fg  = _STATUS_FG[status]
    banner_bg  = _STATUS_BG[status]
    dq_display = _format_dq(payload["overall_dq_pct"])
    table_rows = _render_table_rows(payload["table_results"])
    dq_pct     = payload["overall_dq_pct"]
    dq_color   = (
        "#1a7a4a" if dq_pct is not None and dq_pct >= 90.0 else
        "#d97706" if dq_pct is not None and dq_pct >= 70.0 else
        "#c0392b"
    )

    # Optional CTA button — only rendered when run_link is available
    run_link_btn = ""
    if payload.get("run_link"):
        run_link_btn = f"""
  <tr><td style="padding:0 32px 28px;">
    <table cellpadding="0" cellspacing="0" width="100%"><tr><td align="center">
      <a href="{payload['run_link']}"
         style="display:inline-block;background:#2563eb;color:#fff;
                padding:11px 28px;border-radius:6px;text-decoration:none;
                font-size:14px;font-weight:600;letter-spacing:.2px;">
        View Pipeline Run &#x2192;
      </a>
    </td></tr></table>
  </td></tr>"""

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:Arial,Helvetica,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:36px 0;">
<tr><td align="center">
<table width="620" cellpadding="0" cellspacing="0"
       style="background:#ffffff;border-radius:10px;overflow:hidden;">
  <tr>
    <td style="background:{accent};padding:28px 32px 24px;">
      <div style="color:rgba(255,255,255,.7);font-size:10px;letter-spacing:1.5px;
                  text-transform:uppercase;margin-bottom:6px;">
        MDIF Pipeline &#xB7; Automated Notification
      </div>
      <div style="color:#fff;font-size:22px;font-weight:700;">{payload['pipeline_name']}</div>
      <div style="color:rgba(255,255,255,.65);font-size:12px;margin-top:6px;">
        {payload['run_timestamp']} &nbsp;&#xB7;&nbsp;
        Run ID: <code style="color:rgba(255,255,255,.85);">{payload['run_id']}</code>
      </div>
    </td>
  </tr>
  <tr>
    <td style="background:{banner_bg};padding:14px 32px;border-bottom:1px solid #e9ecef;">
      <table width="100%" cellpadding="0" cellspacing="0"><tr>
        <td style="font-size:16px;font-weight:700;color:{banner_fg};">
          {_STATUS_EMOJI[status]}&nbsp; {status}
        </td>
        <td align="right" style="font-size:12px;color:#555;">
          {payload['succeeded_tables']} of {payload['total_tables']} tables &nbsp;&#xB7;&nbsp;
          Overall DQ: <strong style="color:{dq_color};">{dq_display}</strong>
        </td>
      </tr></table>
    </td>
  </tr>
  <tr>
    <td style="padding:28px 32px 20px;">
      <table width="100%" cellpadding="0" cellspacing="0"><tr>
        <td width="31%" style="background:#f0fdf4;border-radius:8px;padding:16px 12px;text-align:center;">
          <div style="font-size:30px;font-weight:700;color:#1a7a4a;">{payload['succeeded_tables']}</div>
          <div style="font-size:10px;color:#555;text-transform:uppercase;letter-spacing:.8px;margin-top:6px;">Ingested</div>
        </td>
        <td width="5%"></td>
        <td width="31%" style="background:#fef2f2;border-radius:8px;padding:16px 12px;text-align:center;">
          <div style="font-size:30px;font-weight:700;color:#c0392b;">{payload['failed_tables']}</div>
          <div style="font-size:10px;color:#555;text-transform:uppercase;letter-spacing:.8px;margin-top:6px;">Failed</div>
        </td>
        <td width="5%"></td>
        <td width="31%" style="background:#eff6ff;border-radius:8px;padding:16px 12px;text-align:center;">
          <div style="font-size:30px;font-weight:700;color:{dq_color};">{dq_display}</div>
          <div style="font-size:10px;color:#555;text-transform:uppercase;letter-spacing:.8px;margin-top:6px;">Data Quality</div>
        </td>
      </tr></table>
    </td>
  </tr>
  <tr>
    <td style="padding:0 32px 28px;">
      <div style="font-size:13px;font-weight:700;color:#333;margin-bottom:10px;">Table Details</div>
      <table width="100%" cellpadding="0" cellspacing="0"
             style="border:1px solid #e9ecef;border-radius:8px;overflow:hidden;font-size:13px;">
        <thead>
          <tr style="background:#f8f9fa;border-bottom:2px solid #e9ecef;">
            <th style="padding:10px 12px;text-align:left;color:#555;font-weight:600;
                       font-size:11px;text-transform:uppercase;letter-spacing:.5px;">Table</th>
            <th style="padding:10px 12px;text-align:center;color:#555;font-weight:600;
                       font-size:11px;text-transform:uppercase;letter-spacing:.5px;">Status</th>
            <th style="padding:10px 12px;text-align:right;color:#555;font-weight:600;
                       font-size:11px;text-transform:uppercase;letter-spacing:.5px;">Rows</th>
            <th style="padding:10px 12px;text-align:center;color:#555;font-weight:600;
                       font-size:11px;text-transform:uppercase;letter-spacing:.5px;">DQ %</th>
          </tr>
        </thead>
        <tbody>{table_rows}</tbody>
      </table>
    </td>
  </tr>
  {run_link_btn}
  <tr>
    <td style="padding:16px 32px 20px;border-top:1px solid #f0f0f0;">
      <div style="font-size:11px;color:#aaa;text-align:center;line-height:1.6;">
        Automated notification from the MDIF Data Platform.<br>
        Do not reply to this email.
      </div>
    </td>
  </tr>
</table>
</td></tr></table>
</body></html>"""


def _build_subject(payload):
    """Build the email subject line: emoji + pipeline name + status + counts + DQ."""
    emoji  = _STATUS_EMOJI[payload["overall_status"]]
    status = payload["overall_status"]
    dq     = _format_dq(payload["overall_dq_pct"])
    counts = f"{payload['succeeded_tables']}/{payload['total_tables']} tables"
    return f"{emoji} [MDIF] {payload['pipeline_name']} — {status} | {counts} | DQ: {dq}"


def trigger_logic_app(logic_app_url, to_addresses, subject, html_body):
    """POST the email payload to the Azure Logic App HTTP trigger. Raises on non-2xx."""
    response = requests.post(
        logic_app_url,
        json={"to": ";".join(to_addresses), "subject": subject, "body": html_body},
        timeout=30,
    )
    response.raise_for_status()


def notify_pipeline_run(pipeline_name, run_id, run_link, table_results,
                        notify_config, logic_app_url, run_timestamp=None, log_fn=None):
    """End-to-end notification: resolve receivers, build payload, render HTML, and send.

    Raises ValueError if no valid recipients can be resolved from notify_config.
    """
    to_addresses = resolve_receivers(notify_config, log_fn=log_fn)

    if not to_addresses:
        raise ValueError(
            f"[notify] No receivers resolved for '{pipeline_name}' — "
            f"notify_config={notify_config} — check control table notify column"
        )

    payload   = build_email_payload(pipeline_name, run_id, run_link, table_results, run_timestamp)
    subject   = _build_subject(payload)
    html_body = render_email_html(payload)
    trigger_logic_app(logic_app_url, to_addresses, subject, html_body)