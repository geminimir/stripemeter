/*
 Credits Lifecycle Scenario
 - Demonstrates issuing credits (entitlements) and burning them with usage
 - Handles burn order (oldest-first), expiry, and optional rollover
 - Prints a concise burn report and overage beyond credits
 - Best-effort emits usage events to StripeMeter if backend is up
*/

const { formatISO, addDays, isAfter, isBefore, parseISO, min, startOfDay } = require('date-fns');

let StripeMeterClient;
try {
  StripeMeterClient = require('../src/stripemeter-client');
} catch (_) {
  StripeMeterClient = class { async track() {} };
}

const { DEMO_USERS } = require('../src/demo-config');

function arg(name, def) {
  const idx = process.argv.indexOf(`--${name}`);
  if (idx !== -1 && idx + 1 < process.argv.length) return process.argv[idx + 1];
  return def;
}

function pickUser(key) {
  const map = Object.fromEntries(DEMO_USERS.map(u => [u.name.split(' ')[0].toLowerCase(), u]));
  return map[key] || DEMO_USERS[0];
}

function within(day, start, end) {
  return (isAfter(day, start) || +day === +start) && (isBefore(day, end) || +day === +end);
}

function clone(obj) { return JSON.parse(JSON.stringify(obj)); }

async function maybeTrack(stripeMeter, customerRef, ts, quantity) {
  try {
    await stripeMeter.track({
      metric: 'api_calls',
      customerRef,
      quantity,
      ts,
      meta: { scenario: 'credits_lifecycle' },
    });
  } catch (_) {}
}

function issueCredits(credits, grant) {
  credits.push({
    id: grant.id,
    remaining: grant.amount,
    amount: grant.amount,
    unit: grant.unit,
    metric: grant.metric,
    validFrom: grant.validFrom,
    validTo: grant.validTo,
    rollover: grant.rollover,
    burned: 0,
    expired: 0,
    rolled: 0,
  });
}

function burnCreditsFIFO(credits, when, amount) {
  let toBurn = amount;
  let burned = 0;
  // Sort by validFrom (oldest first)
  credits.sort((a, b) => new Date(a.validFrom) - new Date(b.validFrom));
  for (const c of credits) {
    if (toBurn <= 0) break;
    const day = startOfDay(when);
    if (!within(day, startOfDay(parseISO(c.validFrom)), startOfDay(parseISO(c.validTo)))) continue;
    const take = Math.min(c.remaining, toBurn);
    if (take > 0) {
      c.remaining -= take;
      c.burned += take;
      burned += take;
      toBurn -= take;
    }
  }
  return { burned, overage: toBurn };
}

function expireAndRollover(credits, boundaryDay) {
  let expiredTotal = 0;
  let rolledTotal = 0;
  for (const c of credits) {
    const day = startOfDay(boundaryDay);
    const validTo = startOfDay(parseISO(c.validTo));
    if (+day > +validTo && c.remaining > 0) {
      if (c.rollover) {
        rolledTotal += c.remaining;
        c.rolled += c.remaining;
        // For demo, extend validTo by 30 days on rollover
        const newEnd = addDays(validTo, 30);
        c.validFrom = formatISO(addDays(validTo, 1));
        c.validTo = formatISO(newEnd);
        // remaining keeps same amount
      } else {
        expiredTotal += c.remaining;
        c.expired += c.remaining;
        c.remaining = 0;
      }
    }
  }
  return { expiredTotal, rolledTotal };
}

async function run() {
  const userKey = arg('user', 'alice');
  const user = pickUser(userKey);
  const customerRef = user.stripeCustomerId;
  const start = startOfDay(parseISO(arg('start', formatISO(startOfDay(new Date())))));
  const days = Number(arg('days', '40'));
  const dailyCalls = Number(arg('dailyCalls', '1500'));

  const stripeMeter = new StripeMeterClient({
    apiUrl: process.env.STRIPEMETER_API_URL || 'http://localhost:3000',
    tenantId: process.env.TENANT_ID || 'demo-tenant-001',
  });

  // Define credit grants
  const grants = [
    {
      id: 'grant_A',
      amount: 50000, // 50k calls
      unit: 'call',
      metric: 'api_calls',
      validFrom: formatISO(start),
      validTo: formatISO(addDays(start, 30)),
      rollover: true,
    },
    {
      id: 'grant_B',
      amount: 20000, // 20k calls
      unit: 'call',
      metric: 'api_calls',
      validFrom: formatISO(addDays(start, 10)),
      validTo: formatISO(addDays(start, 40)),
      rollover: false,
    },
  ];

  const credits = [];
  for (const g of grants) issueCredits(credits, g);

  let totalUsage = 0;
  let totalBurned = 0;
  let totalOverage = 0;

  for (let i = 0; i < days; i++) {
    const when = addDays(start, i);
    const ts = formatISO(when);
    totalUsage += dailyCalls;
    const { burned, overage } = burnCreditsFIFO(credits, when, dailyCalls);
    totalBurned += burned;
    totalOverage += overage;
    await maybeTrack(stripeMeter, customerRef, ts, dailyCalls);

    // At grant_A boundary, process expiry/rollover
    if (i === 30) expireAndRollover(credits, when);
  }

  // Final expiry pass at end of window
  expireAndRollover(credits, addDays(start, days));

  const snapshot = credits.map(c => ({
    id: c.id,
    amount: c.amount,
    burned: c.burned,
    remaining: c.remaining,
    expired: c.expired,
    rolled: c.rolled,
    validFrom: c.validFrom,
    validTo: c.validTo,
  }));

  console.log('\n=== Credits Lifecycle Report ===');
  console.log(`User: ${user.name} (${customerRef})`);
  console.log(`Window: ${formatISO(start)} for ${days} days`);
  console.log('Grants:');
  for (const g of grants) {
    console.log(`- ${g.id}: ${g.amount} ${g.unit} (rollover=${g.rollover}) valid ${g.validFrom} -> ${g.validTo}`);
  }
  console.log('');
  console.log(`Usage total: ${totalUsage}`);
  console.log(`Burned via credits: ${totalBurned}`);
  console.log(`Overage (beyond credits): ${totalOverage}`);
  console.log('');
  console.log('Credits state:');
  snapshot.forEach(s => {
    console.log(`- ${s.id}: burned=${s.burned} remaining=${s.remaining} expired=${s.expired} rolled=${s.rolled}`);
  });
  console.log('================================\n');
}

run().catch((e) => {
  console.error('Credits lifecycle failed:', e);
  process.exit(1);
});


