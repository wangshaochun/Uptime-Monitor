import { Bindings, Monitor } from '../types';
import { sendAlertToAllChannels } from './notifier';

export async function checkExpiryAlerts(env: Bindings) {
  console.log('Checking expiry alerts...');
  try {
    const { results } = await env.DB.prepare(
      `SELECT id, name, url, status, last_check, cert_expiry, domain_expiry,
              check_ssl, check_domain,
              alert_silence_ssl, alert_silence_domain,
              last_alert_ssl, last_alert_domain
       FROM monitors WHERE paused = 0`
    ).all<Pick<Monitor, 'id' | 'name' | 'url' | 'status' | 'last_check' | 'cert_expiry' | 'domain_expiry' | 'check_ssl' | 'check_domain' | 'alert_silence_ssl' | 'alert_silence_domain' | 'last_alert_ssl' | 'last_alert_domain'>>();

    const now = Date.now();
    const tasks = results.map(async (monitor) => {
      // A successful HTTPS check means Cloudflare has already validated the
      // live certificate chain. If the stored cert_expiry is in the past while
      // the monitor is UP over HTTPS, the stored value is stale (e.g. crt.sh
      // returned an old certificate), not the real certificate. Don't send a
      // false "expired" alert in that case.
      const lastCheckMs = monitor.last_check ? new Date(monitor.last_check).getTime() : 0;
      const liveHttpsUp = /^https:\/\//i.test(monitor.url || '')
        && monitor.status === 'UP'
        && now - lastCheckMs < 6 * 3_600_000;

      const checks = [
        { label: 'SSL 证书', dateStr: monitor.cert_expiry, enabled: (monitor.check_ssl ?? 1) === 1, silenceHours: monitor.alert_silence_ssl ?? 24, lastAlertAt: monitor.last_alert_ssl, lastAlertField: 'last_alert_ssl' },
        { label: '域名', dateStr: monitor.domain_expiry, enabled: (monitor.check_domain ?? 1) === 1, silenceHours: monitor.alert_silence_domain ?? 24, lastAlertAt: monitor.last_alert_domain, lastAlertField: 'last_alert_domain' },
      ];
      for (const check of checks) {
        // Certificate expiry is meaningless for plain-HTTP monitors.
        if (check.lastAlertField === 'last_alert_ssl' && !/^https:\/\//i.test(monitor.url || '')) continue;
        if (!check.enabled || !check.dateStr) continue;
        const lastMs = check.lastAlertAt ? new Date(check.lastAlertAt).getTime() : 0;
        if (check.silenceHours > 0 && (now - lastMs) < check.silenceHours * 3_600_000) continue;
        const daysLeft = Math.ceil((new Date(check.dateStr).getTime() - now) / (1000 * 60 * 60 * 24));
        let detail = '';
        if (daysLeft <= 0) {
          if (check.lastAlertField === 'last_alert_ssl' && liveHttpsUp) {
            console.warn(`[expiry] Skip stale SSL alert for ${monitor.name} (${monitor.url}): stored expiry ${check.dateStr}, but live HTTPS check is UP`);
            continue;
          }
          detail = `❌ ${check.label}已过期，请立即续期处理！`;
        }
        else if (daysLeft <= 7) detail = `🚨 ${check.label}紧急预警，仅剩 ${daysLeft} 天到期，请尽快续期！`;
        else if (daysLeft <= 30) detail = `⏰ ${check.label}到期提醒，还有 ${daysLeft} 天到期，请注意续期。`;
        if (detail) {
          const sent = await sendAlertToAllChannels(env, monitor as Monitor, 'DOWN', detail);
          if (sent) await env.DB.prepare(`UPDATE monitors SET ${check.lastAlertField} = ? WHERE id = ?`)
            .bind(new Date().toISOString(), monitor.id).run();
        }
      }
    });
    await Promise.all(tasks);
    console.log('Expiry alert check completed.');
  } catch (e: unknown) {
    console.error('Expiry alert check error:', e);
  }
}
