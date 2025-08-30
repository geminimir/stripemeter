# CloudAPI Demo - StripeMeter Showcase

> **A realistic SaaS demo showcasing StripeMeter's usage metering and billing transparency**

This demo simulates **CloudAPI**, a fictional API-as-a-Service platform that uses StripeMeter for transparent usage-based billing.

## 🎯 What This Demo Shows

### **Real-World Usage Patterns**
- **API Gateway** with rate limiting and usage tracking
- **Multiple pricing tiers** (Free, Pro, Enterprise)
- **Different metrics** (API calls, data processed, storage used)
- **Live cost calculations** with real-time updates

### **StripeMeter Integration**
- **Automatic usage tracking** for every API call
- **Real-time cost projections** visible to customers
- **Transparent billing** with detailed breakdowns
- **Stripe synchronization** with exact parity

### **Customer Experience**
- **Live usage dashboard** showing current consumption
- **Cost predictions** based on usage patterns
- **Billing transparency** - no surprises at month-end
- **Usage alerts** when approaching limits

## 🏗️ Demo Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CloudAPI      │    │   StripeMeter   │    │   Customer      │
│   Demo Service  │────▶│   Backend       │────▶│   Dashboard     │
│                 │    │                 │    │                 │
│ • REST API      │    │ • Usage Events  │    │ • Live Metrics  │
│ • Rate Limits   │    │ • Aggregation   │    │ • Cost Preview  │
│ • Auth/API Keys │    │ • Stripe Sync   │    │ • Billing UI    │
│ • Usage Sim     │    │ • Reconciliation│    │ • Usage History │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🚀 Quick Start

### 1. Start StripeMeter Backend
```bash
# From project root
cd ../..
docker compose up -d
pnpm dev
```

### 2. Run CloudAPI Demo
```bash
cd demo/cloudapi-saas
npm install
npm run dev
```

### 3. View Customer Dashboard
```bash
cd customer-dashboard
npm install
npm start
```

### 4. Access Demo
- **CloudAPI Service**: http://localhost:4000
- **Customer Dashboard**: http://localhost:3001
- **StripeMeter Admin**: http://localhost:3000

## 💡 Demo Scenarios

### **Scenario 1: Free Tier User**
- **Limit**: 1,000 API calls/month
- **Demo**: Make API calls, watch usage counter
- **Experience**: See approaching limit warnings

### **Scenario 2: Pro Tier User**  
- **Pricing**: $0.01 per API call after 10,000 free
- **Demo**: Simulate high usage, see cost calculations
- **Experience**: Real-time cost projections

### **Scenario 3: Enterprise User**
- **Pricing**: Tiered pricing with volume discounts
- **Demo**: Bulk API usage simulation
- **Experience**: Complex pricing transparency

### **Scenario 4: Overage Handling**
- **Demo**: Exceed plan limits
- **Experience**: Automatic overage billing, clear notifications

## 📊 Pricing Model (Demo)

### **Free Plan**
- 1,000 API calls/month
- Basic support
- Community features

### **Pro Plan - $29/month**
- 10,000 API calls included
- $0.01 per additional call
- Priority support
- Advanced analytics

### **Enterprise Plan - $299/month**
- 100,000 API calls included
- Tiered pricing:
  - Next 400,000 calls: $0.008 each
  - Next 500,000 calls: $0.006 each
  - 1M+ calls: $0.004 each
- Dedicated support
- Custom integrations

## 🎮 Interactive Features

### **API Playground**
- **Live API testing** with usage tracking
- **Rate limit visualization** 
- **Real-time cost updates**

### **Usage Simulator**
- **Traffic patterns**: Steady, burst, seasonal
- **Bulk operations**: Simulate high-volume usage
- **Multi-metric tracking**: Calls + data + storage

### **Dunning Lab (Failed Payment Scenarios)**
- Simulates lifecycle: past_due → email_sent → retry_scheduled → blocked → recovered
- Adds headers on at-risk requests and blocks when in hard dunning
- Records dunning events via `billing_event` metric for auditability

#### Run the Dunning Lab
```bash
cd demo/cloudapi-saas
npm run dev   # in one terminal

# in another terminal
npm run dunning            # defaults: user=alice speed=1s
node scripts/dunning-lab.js --user bob --speed 2
```
Expected:
- Before block: requests succeed with `X-Dunning-Warning: true`
- Blocked: requests return 402 Payment Required
- After resolve: requests succeed normally

### **Credits Lifecycle (Entitlements)**
- Issues credits (e.g., 50k API calls), burns FIFO by usage, handles expiry and optional rollover
- Shows remaining, burned, expired, rolled credits, and overage beyond credits

#### Run the Credits Demo
```bash
cd demo/cloudapi-saas
node scripts/credits-lifecycle.js --user alice --days 40 --dailyCalls 1500
```
Expected:
- Report prints grants, window, totals
- Burned first until credits exhausted; any remainder is overage
- Rollover grants extend validity; non-rollover expire

### **Customer Portal**
- **Usage dashboard** with charts and metrics
- **Cost projections** based on current usage
- **Billing history** with detailed breakdowns
- **Usage alerts** and notifications

## 📱 Demo Screenshots

### Customer Dashboard
![Usage Dashboard](./screenshots/dashboard.png)
*Real-time usage tracking with cost projections*

### API Playground
![API Testing](./screenshots/playground.png)
*Test APIs while watching usage and costs update live*

### Billing Transparency
![Billing Details](./screenshots/billing.png)
*Detailed cost breakdown with Stripe parity*

## 🎯 Value Demonstration

### **For SaaS Founders**
- **Customer Trust**: Transparent billing reduces churn
- **Revenue Optimization**: Clear usage patterns inform pricing
- **Support Reduction**: Fewer billing-related support tickets

### **For Developers**
- **Easy Integration**: Simple SDK, clear documentation
- **Real-time Data**: Live usage tracking and cost calculation
- **Production Ready**: Handles edge cases, rate limits, errors

### **For Customers**
- **No Surprises**: Always know what you'll be charged
- **Usage Control**: Set alerts and limits
- **Fair Pricing**: Pay for exactly what you use

## 🔧 Technical Implementation

### **Usage Tracking**
```javascript
// Automatic usage tracking on every API call
app.use('/api/*', async (req, res, next) => {
  // Track API call
  await stripeMeter.track({
    metric: 'api_calls',
    customerRef: req.user.stripeCustomerId,
    quantity: 1,
    meta: {
      endpoint: req.path,
      method: req.method,
      user_agent: req.get('User-Agent')
    }
  });
  
  next();
});
```

### **Real-time Cost Calculation**
```javascript
// Get live usage and cost projection
const projection = await stripeMeter.getProjection(customerId);
// Returns: { current: 15420, projected: 18500, cost: 45.20 }
```

### **Usage Limits & Alerts**
```javascript
// Set up usage alerts
await stripeMeter.createAlert({
  customerId,
  metric: 'api_calls',
  threshold: 8000, // 80% of 10k limit
  action: 'email'
});
```

## 📈 Demo Metrics

The demo tracks these key metrics to showcase StripeMeter's capabilities:

- **API Calls**: Primary usage metric
- **Data Processed**: GB of request/response data
- **Storage Used**: Persistent data storage
- **Compute Time**: Processing time for complex requests

## 🎪 Live Demo Script

### **5-Minute Demo Flow**
1. **Show customer dashboard** - current usage, costs
2. **Make API calls** - watch counters update in real-time
3. **Simulate high usage** - see cost projections change
4. **Show billing breakdown** - detailed, transparent pricing
5. **Demonstrate alerts** - usage limit notifications

### **Demo Talking Points**
- "Watch the cost update in real-time as we make API calls"
- "Customers always know exactly what they'll be charged"
- "No more billing surprises or support tickets about unexpected charges"
- "This is the level of transparency your customers deserve"

## 🚀 Next Steps

After seeing this demo:

1. **Try StripeMeter** with your own use case
2. **Join our community** - contribute and get support
3. **Book a consultation** - discuss your specific needs
4. **Star the repo** - help spread transparent billing

---

**Ready to give your customers billing transparency?**

[Get Started with StripeMeter →](../../README.md)
