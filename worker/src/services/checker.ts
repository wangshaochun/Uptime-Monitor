import { Bindings, Monitor } from '../types';
import { renderAlertDetail, sendAlertToAllChannels } from './notifier';

export async function checkSites(env: Bindings) {
  console.log('Starting scheduled check...');
  const now = Date.now();
  let results: Monitor[] = [];
  try {
    const res = await env.DB.prepare('SELECT * FROM monitors').all<Monitor>();
    results = res.results || [];
  } catch (e) {
    console.error('Error fetching monitors in checkSites:', e);
    return;
  }
  const tasks = results.map(async (monitor) => {
    if (monitor.paused === 1) return;
    if (isTimeToCheck(monitor, now)) await performCheck(monitor, env);
  });
  await Promise.all(tasks);
}

export function isTimeToCheck(monitor: Monitor, now: number): boolean {
  if (monitor.status === 'RETRYING') return true;
  const lastCheck = monitor.last_check ? new Date(monitor.last_check).getTime() : 0;
  const intervalMs = (monitor.interval || 300) * 1000;
  return now - lastCheck >= intervalMs;
}

export function isExpectedStatusCode(status: number, expectedConfig: string | null): boolean {
  if (!expectedConfig || !expectedConfig.trim()) {
    return status >= 200 && status < 300;
  }
  const parts = expectedConfig.split(',').map(p => p.trim()).filter(Boolean);
  for (const part of parts) {
    if (part.includes('-')) {
      const [start, end] = part.split('-').map(Number);
      if (!isNaN(start) && !isNaN(end) && status >= start && status <= end) {
        return true;
      }
    } else {
      const num = Number(part);
      if (!isNaN(num) && status === num) {
        return true;
      }
    }
  }
  return false;
}

export async function performCheck(monitor: Monitor, env: Bindings) {
  const startTime = Date.now();
  let status = 200;
  let isFail = false;
  let reason = '';

  try {
    let headers: Record<string, string> = {
      'User-Agent': monitor.user_agent || 'Uptime-Monitor/1.0',
    };
    if (monitor.request_headers) {
      try {
        const customHeaders = JSON.parse(monitor.request_headers) as Record<string, string>;
        headers = { ...headers, ...customHeaders };
      } catch { /* ignore malformed headers */ }
    }

    const fetchOptions: RequestInit = {
      method: monitor.method || 'GET',
      headers,
      cf: { cacheTtl: 0, cacheEverything: false } as RequestInitCfProperties,
    };

    if (['POST', 'PUT', 'PATCH'].includes(monitor.method || 'GET') && monitor.request_body) {
      fetchOptions.body = monitor.request_body;
      if (!headers['Content-Type']) {
        (fetchOptions.headers as Record<string, string>)['Content-Type'] = 'application/json';
      }
    }

    const response = await fetch(monitor.url, fetchOptions);
    status = response.status;

    if (!isExpectedStatusCode(status, monitor.expected_codes)) {
      isFail = true;
      reason = `HTTP ${status}`;
    } else {
      if (monitor.keyword) {
        const text = await response.text();
        if (!text.includes(monitor.keyword)) {
          isFail = true;
          reason = `Keyword "${monitor.keyword}" not found`;
        }
      }

      const lastInfoCheck = monitor.check_info_status ? new Date(monitor.check_info_status).getTime() : 0;
      const certCheckEnabled = (monitor.check_ssl ?? 1) !== 0 || (monitor.check_domain ?? 1) !== 0;
      if (certCheckEnabled && Date.now() - lastInfoCheck > 86400000) {
        // Keep this awaited: a fire-and-forget promise can be cancelled when the
        // scheduled invocation finishes, leaving a stale cert_expiry in the DB.
        await env.DB.prepare('UPDATE monitors SET check_info_status = ? WHERE id = ?')
          .bind(new Date().toISOString(), monitor.id).run();
        await updateDomainCertInfo(env, monitor);
      }
    }
  } catch (e: unknown) {
    isFail = true;
    status = 0;
    const errorMsg = e instanceof Error ? e.message : 'Unknown error';
    if (errorMsg.includes('handshake') || errorMsg.includes('certificate') || errorMsg.includes('SSL') || errorMsg.includes('TLS')) {
      reason = `SSL Error: ${errorMsg}`;
    } else if (errorMsg.includes('time') || errorMsg.includes('timeout')) {
      reason = 'Timeout';
    } else {
      reason = errorMsg || 'Network Error';
    }
  }

  const latency = Date.now() - startTime;

  await env.DB.prepare('INSERT INTO logs (monitor_id, status_code, latency, is_fail, reason) VALUES (?, ?, ?, ?, ?)')
    .bind(monitor.id, status, latency, isFail ? 1 : 0, reason || null).run();

  if (!isFail && monitor.alert_error_rate > 0) {
    await checkErrorRateAlert(env, monitor);
  }

  let newStatus: Monitor['status'] = monitor.status;
  let newRetryCount = monitor.retry_count;

  try {
    const { results: activeMaint } = await env.DB.prepare(
      "SELECT affected_monitors FROM incidents WHERE type = 'maintenance' AND status = 'active' AND datetime(scheduled_start) <= datetime('now') AND datetime(scheduled_end) >= datetime('now')"
    ).all<{ affected_monitors: string | null }>();
    if (activeMaint && activeMaint.length > 0) {
      const inMaintenance = activeMaint.some(m => {
        if (!m.affected_monitors) return false;
        return m.affected_monitors.split(',').map(s => s.trim()).includes(String(monitor.id));
      });
      if (inMaintenance) {
        await env.DB.prepare('UPDATE monitors SET last_check = ?, status = ?, retry_count = ? WHERE id = ?')
          .bind(new Date().toISOString(), newStatus, newRetryCount, monitor.id).run();
        return;
      }
    }
  } catch { /* ignore maintenance check errors */ }

  const silenceHoursUptime = monitor.alert_silence_uptime ?? 24;
  const lastAlertUptimeMs = monitor.last_alert_uptime ? new Date(monitor.last_alert_uptime).getTime() : 0;
  const silenced = silenceHoursUptime > 0 && (Date.now() - lastAlertUptimeMs) < silenceHoursUptime * 3_600_000;

  if (isFail) {
    if (monitor.status === 'UP') {
      newStatus = 'RETRYING';
      newRetryCount = 1;
    } else if (monitor.status === 'RETRYING') {
      if (newRetryCount < 3) {
        newRetryCount++;
      } else {
        newStatus = 'DOWN';
        if (!silenced) {
          const detail = await renderAlertDetail(env, 'alert_template_down', '错误原因: {reason}', {
            reason,
            status: String(status),
            latency: String(latency),
            type: 'DOWN',
          }, monitor);
          const sent = await sendAlertToAllChannels(env, monitor, 'DOWN', detail);
          if (sent) await env.DB.prepare('UPDATE monitors SET last_alert_uptime = ? WHERE id = ?')
            .bind(new Date().toISOString(), monitor.id).run();
        }
      }
    }
  } else {
    if (monitor.status === 'DOWN') {
      const detail = await renderAlertDetail(env, 'alert_template_up', '响应耗时: {latency}ms', {
        reason,
        status: String(status),
        latency: String(latency),
        type: 'UP',
      }, monitor);
      const sent = await sendAlertToAllChannels(env, monitor, 'UP', detail);
      if (sent) await env.DB.prepare('UPDATE monitors SET last_alert_uptime = ? WHERE id = ?')
        .bind(new Date().toISOString(), monitor.id).run();
    }
    newStatus = 'UP';
    newRetryCount = 0;
  }

  await env.DB.prepare('UPDATE monitors SET last_check = ?, status = ?, retry_count = ? WHERE id = ?')
    .bind(new Date().toISOString(), newStatus, newRetryCount, monitor.id).run();
}

export async function checkErrorRateAlert(env: Bindings, monitor: Monitor) {
  try {
    const since = new Date(Date.now() - 5 * 60_000).toISOString();
    const row = await env.DB.prepare(
      'SELECT COUNT(*) as total, SUM(is_fail) as fails FROM logs WHERE monitor_id = ? AND created_at >= ?'
    ).bind(monitor.id, since).first<{ total: number; fails: number }>();

    if (!row || row.total < 3) return;

    const errorRate = Math.round((row.fails / row.total) * 100);
    if (errorRate >= monitor.alert_error_rate) {
      const silenceHoursUptime = monitor.alert_silence_uptime ?? 24;
      const lastAlertMs = monitor.last_alert_uptime ? new Date(monitor.last_alert_uptime).getTime() : 0;
      if (silenceHoursUptime > 0 && (Date.now() - lastAlertMs) < silenceHoursUptime * 3_600_000) return;

      const detail = await renderAlertDetail(
        env,
        'alert_template_error_rate',
        '错误率告警：过去 5 分钟内错误率 {error_rate}%，超过阈值 {threshold}%',
        {
          error_rate: String(errorRate),
          threshold: String(monitor.alert_error_rate),
          type: 'DOWN',
        },
        monitor
      );
      const sent = await sendAlertToAllChannels(env, monitor, 'DOWN', detail);
      if (sent) await env.DB.prepare('UPDATE monitors SET last_alert_uptime = ? WHERE id = ?')
        .bind(new Date().toISOString(), monitor.id).run();
    }
  } catch (e) {
    console.error('Error rate check failed:', e);
  }
}

type CertEntry = {
  not_before?: string | null;
  not_after?: string | null;
  name_value?: string | null;
  common_name?: string | null;
};

type RdapDomain = {
  events?: { eventAction?: string; eventDate?: string }[];
};

const CERT_LOOKUP_TIMEOUT_MS = 6_000;
const CERT_LOOKUP_ATTEMPTS = 2;
const RDAP_LOOKUP_TIMEOUT_MS = 8_000;

// Common multi-label public suffixes. This doesn't need to be exhaustive: the
// registrable domain is only used for RDAP and wildcard-cert fallback, and the
// exact hostname is always queried first.
const MULTI_LEVEL_TLDS = new Set([
  'com.cn', 'net.cn', 'org.cn', 'gov.cn', 'edu.cn', 'ac.cn',
  'co.uk', 'org.uk', 'me.uk', 'ac.uk', 'gov.uk',
  'com.hk', 'org.hk', 'net.hk', 'edu.hk',
  'com.tw', 'org.tw', 'net.tw',
  'com.au', 'net.au', 'org.au',
  'co.jp', 'co.kr', 'co.nz', 'co.in', 'co.za', 'co.il',
  'com.br', 'com.mx', 'com.ar', 'com.tr', 'com.ru',
  'com.pl', 'com.es', 'com.it', 'com.fr', 'com.de', 'com.sg',
]);

function getRootDomain(hostname: string): string {
  const parts = hostname.split('.').filter(Boolean);
  if (parts.length <= 2) return hostname;
  const lastTwo = parts.slice(-2).join('.');
  if (MULTI_LEVEL_TLDS.has(lastTwo) && parts.length >= 3) {
    return parts.slice(-3).join('.');
  }
  return lastTwo;
}

function parseCertificateDate(value: string | null | undefined): number {
  if (!value) return NaN;
  const normalized = value.trim().replace(' ', 'T');
  // CT/RDAP timestamps are UTC. Keep timezone-less legacy values UTC as well.
  const withZone = /(?:Z|[+-]\d{2}:?\d{2})$/.test(normalized) ? normalized : `${normalized}Z`;
  return new Date(withZone).getTime();
}

function getCertificateNames(entry: CertEntry): string[] {
  return `${entry.name_value || ''}\n${entry.common_name || ''}`
    .split(/\s+/)
    .map(name => name.trim().toLowerCase())
    .filter(Boolean);
}

function certificateCoversHost(entry: CertEntry, hostname: string): boolean {
  return getCertificateNames(entry).some(name => {
    if (name === hostname) return true;
    if (!name.startsWith('*.')) return false;
    const suffix = name.slice(1); // ".example.com"
    if (!hostname.endsWith(suffix)) return false;
    const label = hostname.slice(0, hostname.length - suffix.length);
    return label.length > 0 && !label.includes('.');
  });
}

function selectCertExpiry(entries: CertEntry[], hostname: string, now: number): string | null {
  const candidates = entries
    .map(entry => ({
      entry,
      notBefore: parseCertificateDate(entry.not_before),
      notAfter: parseCertificateDate(entry.not_after),
    }))
    .filter(x =>
      !isNaN(x.notAfter) &&
      x.notAfter > now &&
      certificateCoversHost(x.entry, hostname)
    );

  // Never fall back to an expired certificate here. If CT logs are stale, the
  // caller should keep the previous value (or report "unknown") instead of
  // storing an already-expired date and triggering a false alarm.
  if (candidates.length === 0) return null;

  // Prefer a certificate issued for the exact hostname. A wildcard certificate
  // may have a later expiry but is less likely to be the one actually served.
  const exact = candidates.filter(x => getCertificateNames(x.entry).includes(hostname));
  const pool = exact.length > 0 ? exact : candidates;

  // CT logs also contain certificates that were issued but never deployed. The
  // most recently issued valid certificate is the best available guess.
  pool.sort((a, b) => (b.notBefore - a.notBefore) || (b.notAfter - a.notAfter));
  const notAfter = pool[0].entry.not_after as string;
  return new Date(notAfter.replace(' ', 'T') + (/[zZ]|[+-]\d{2}:?\d{2}$/.test(notAfter) ? '' : 'Z')).toISOString();
}

function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchWithTimeout(url: string, init: RequestInit, timeoutMs: number): Promise<Response> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function fetchCertsFromCt(searchDomain: string, attempts = CERT_LOOKUP_ATTEMPTS): Promise<CertEntry[] | null> {
  const params = new URLSearchParams({ q: searchDomain, output: 'json' });
  const url = `https://crt.sh/?${params.toString()}`;
  const browserUA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      const res = await fetchWithTimeout(url, {
        headers: { 'User-Agent': browserUA, Accept: 'application/json' },
      }, CERT_LOOKUP_TIMEOUT_MS);

      // crt.sh returns 404 both for "no results" and when it is overloaded.
      // Retry first; after the budget is exhausted, treat it as no data.
      if (res.status === 404) {
        if (attempt < attempts) {
          await delay(400 * attempt);
          continue;
        }
        return [];
      }
      if (!res.ok) throw new Error(`crt.sh HTTP ${res.status}`);

      const data: unknown = await res.json();
      return Array.isArray(data) ? (data as CertEntry[]) : null;
    } catch (e) {
      console.warn(`crt.sh lookup attempt ${attempt}/${attempts} failed for ${searchDomain}:`, e);
      if (attempt < attempts) await delay(400 * attempt);
    }
  }
  return null;
}

async function fetchRdapDomain(domain: string): Promise<RdapDomain | null> {
  const res = await fetchWithTimeout(`https://rdap.org/domain/${encodeURIComponent(domain)}`, {
    headers: {
      // rdap.org returns 403 to some default/non-browser user agents.
      'User-Agent': 'Uptime-Monitor/1.0',
      Accept: 'application/rdap+json, application/json',
    },
  }, RDAP_LOOKUP_TIMEOUT_MS);

  if (res.status === 404) return null; // e.g. .cn has no public RDAP service
  if (!res.ok) throw new Error(`RDAP HTTP ${res.status}`);
  return res.json<RdapDomain>();
}

type CertSpotterEntry = {
  id?: string;
  not_before?: string;
  not_after?: string;
  dns_names?: string[];
  common_name?: string;
};

/**
 * Cert Spotter fallback. crt.sh is frequently overloaded (502/timeouts), while
 * Cert Spotter returns currently unexpired certificates and has a small free
 * quota (100 requests per hour at the time of writing). Used only when crt.sh
 * fails or returns no usable certificate.
 */
async function fetchCertsFromCertSpotter(
  searchDomain: string,
  includeSubdomains: boolean,
  attempts = 1
): Promise<CertEntry[] | null> {
  const params = new URLSearchParams({
    domain: searchDomain,
    include_subdomains: includeSubdomains ? 'true' : 'false',
    expand: 'dns_names',
  });
  const url = `https://api.certspotter.com/v1/issuances?${params.toString()}`;

  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      const res = await fetchWithTimeout(url, {
        headers: {
          'User-Agent': 'Uptime-Monitor/1.0',
          Accept: 'application/json',
        },
      }, CERT_LOOKUP_TIMEOUT_MS);

      if (!res.ok) throw new Error(`Cert Spotter HTTP ${res.status}`);
      const data: unknown = await res.json();
      if (!Array.isArray(data)) return null;

      return (data as CertSpotterEntry[])
        .filter(entry => entry.not_after)
        .map(entry => ({
          not_before: entry.not_before,
          not_after: entry.not_after,
          name_value: (entry.dns_names || []).join('\n'),
          common_name: entry.common_name,
        }));
    } catch (e) {
      console.warn(`Cert Spotter lookup attempt ${attempt}/${attempts} failed for ${searchDomain}:`, e);
      if (attempt < attempts) await delay(400 * attempt);
    }
  }
  return null;
}

/**
 * Refresh certificate/domain expiry information.
 *
 * Certificate data comes from crt.sh (Certificate Transparency), not from a
 * live TLS handshake. The function therefore only writes a certificate expiry
 * when it found a *currently valid* certificate that covers the monitored
 * hostname; stale/empty/partial CT responses never overwrite a good value.
 *
 * @returns true when at least one expiry field was updated.
 */
export async function updateDomainCertInfo(env: Bindings, monitor: Monitor): Promise<boolean> {
  console.log(`Updating info for ${monitor.url}`);
  try {
    const urlObj = new URL(monitor.url);
    const domain = urlObj.hostname.toLowerCase();
    if (/^\d{1,3}(\.\d{1,3}){3}$/.test(domain) || domain.includes(':')) {
      console.log(`Skipping cert/domain check for IP address: ${domain}`);
      return false;
    }

    const rootDomain = getRootDomain(domain);
    const now = Date.now();
    const isHttps = urlObj.protocol === 'https:';
    const shouldCheckCert = isHttps && (monitor.check_ssl ?? 1) === 1;
    const shouldCheckDomain = (monitor.check_domain ?? 1) === 1;

    let certExpiry: string | null = null;
    let certError = '';

    if (shouldCheckCert) {
      const errors: string[] = [];

      // 1) Exact hostname first. This is the certificate that should be served.
      const exactCt = await fetchCertsFromCt(domain);
      if (exactCt === null) errors.push('crt.sh exact-host lookup failed');
      else certExpiry = selectCertExpiry(exactCt, domain, now);

      if (!certExpiry) {
        const exactSpotter = await fetchCertsFromCertSpotter(domain, false);
        if (exactSpotter === null) {
          errors.push('Cert Spotter exact-host lookup failed');
        } else {
          certExpiry = selectCertExpiry(exactSpotter, domain, now);
          if (certExpiry) console.log(`Using Cert Spotter exact fallback for ${domain}`);
        }
      }

      // 2) Only fall back to wildcard certificates when the exact lookup found
      //    no currently valid certificate.
      if (!certExpiry && rootDomain !== domain) {
        const wildcardCt = await fetchCertsFromCt(`%.${rootDomain}`, 1);
        if (wildcardCt === null) errors.push('crt.sh wildcard lookup failed');
        else certExpiry = selectCertExpiry(wildcardCt, domain, now);

        if (!certExpiry) {
          const wildcardSpotter = await fetchCertsFromCertSpotter(rootDomain, true);
          if (wildcardSpotter === null) {
            errors.push('Cert Spotter wildcard lookup failed');
          } else {
            certExpiry = selectCertExpiry(wildcardSpotter, domain, now);
            if (certExpiry) console.log(`Using Cert Spotter wildcard fallback for ${domain}`);
          }
        }
      }

      if (!certExpiry) {
        certError = errors.length > 0
          ? `${errors.join('; ')}; no currently valid certificate found`
          : 'no currently valid certificate found';
      }

      // Guard against a stale/partial CT response replacing a still-valid date
      // with an older one (e.g. during a renewal window).
      if (certExpiry) {
        const storedMs = parseCertificateDate(monitor.cert_expiry);
        const candidateMs = parseCertificateDate(certExpiry);
        if (!isNaN(storedMs) && storedMs > now && candidateMs < storedMs) {
          console.warn(`Ignoring older cert expiry for ${domain}: candidate ${certExpiry} < stored ${monitor.cert_expiry}`);
          certError = certError ? `${certError}; ignored older CT result` : 'ignored older CT result';
          certExpiry = null;
        }
      }
    }

    // RDAP is keyed by the registrable domain; querying a subdomain returns 404.
    let domainExpiry: string | null = null;
    let domainError = '';
    if (shouldCheckDomain) {
      try {
        const rdapData = await fetchRdapDomain(rootDomain);
        const expEvent = (rdapData?.events || []).find(e => (e.eventAction || '').toLowerCase().includes('expiration'));
        if (expEvent?.eventDate) domainExpiry = expEvent.eventDate;
      } catch (e) {
        domainError = e instanceof Error ? e.message : 'RDAP lookup failed';
      }
    }

    if (certError) console.warn(`Cert info for ${domain}: ${certError}`);
    if (domainError) console.warn(`Domain info for ${rootDomain}: ${domainError}`);

    // Only update fields that were resolved. Do not clear the other field.
    const fields: string[] = [];
    const values: (string | number)[] = [];
    if (certExpiry) { fields.push('cert_expiry = ?'); values.push(certExpiry); }
    if (domainExpiry) { fields.push('domain_expiry = ?'); values.push(domainExpiry); }

    if (fields.length === 0) return false;

    values.push(monitor.id);
    await env.DB.prepare(`UPDATE monitors SET ${fields.join(', ')} WHERE id = ?`).bind(...values).run();
    console.log(`Updated info for ${domain}: Cert=${certExpiry ?? 'unchanged'}, Domain=${domainExpiry ?? 'unchanged'}`);
    return true;
  } catch (e: unknown) {
    console.error('Error in updateDomainCertInfo:', e);
    return false;
  }
}
