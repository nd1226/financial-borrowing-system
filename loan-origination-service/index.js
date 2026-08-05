const express = require('express');
const morgan = require('morgan');
const cors = require('cors');
const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');
const promClient = require('prom-client');
promClient.collectDefaultMetrics();

const app = express();
app.use(express.json());
app.use(morgan('dev'));
app.use(cors());

// Propagate or generate a correlation ID for every request
app.use((req, res, next) => {
  req.correlationId = req.headers['x-correlation-id'] || uuidv4();
  res.set('X-Correlation-ID', req.correlationId);
  next();
});

const apiError = (code, message, details = []) =>
  ({ code, message, ...(details.length && { details }) });

// In-memory stores
const applications = new Map();
const idempotencyStore = new Map(); // idempotency-key → cached response body

// Kafka Configuration
const kafka = new Kafka({
  clientId: 'loan-origination-service',
  brokers: process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'loan-origination-group' });

// Connect to Kafka on startup
async function initKafka() {
  try {
    await producer.connect();
    console.log('Connected to Kafka Producer');
    
    await consumer.connect();
    await consumer.subscribe({ topic: 'CreditDecisionMade', fromBeginning: true });
    
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const event = JSON.parse(message.value.toString());
        console.log(`Received event on ${topic}:`, event);
        
        if (event.applicationId && applications.has(event.applicationId)) {
          const app = applications.get(event.applicationId);
          app.status = event.decision; // 'APPROVED' or 'REJECTED'
          app.creditScore = event.score;
          applications.set(event.applicationId, app);
          console.log(`Updated application ${event.applicationId} to ${app.status}`);
        }
      },
    });
    console.log('Kafka Consumer listening on CreditDecisionMade');
  } catch (error) {
    console.error('Error connecting to Kafka', error);
  }
}
initKafka();

const { authenticateToken } = require('./middleware/auth');

// Health Check
app.get('/health', (req, res) => res.send('OK'));

app.get('/metrics', async (req, res) => {
  res.set('Content-Type', promClient.register.contentType);
  res.end(await promClient.register.metrics());
});

// Create Loan Application (Protected by Keycloak)
app.post('/api/v1/loans', authenticateToken, async (req, res) => {
  const { amount, term, nationalId, name } = req.body;
  if (!amount || !term || !nationalId) {
    return res.status(400).json(apiError('VALIDATION_ERROR', 'Missing required fields', [
      ...(!amount ? ['amount is required'] : []),
      ...(!term ? ['term is required'] : []),
      ...(!nationalId ? ['nationalId is required'] : [])
    ]));
  }

  // Idempotency: return cached response for duplicate keys
  const idempotencyKey = req.headers['idempotency-key'];
  if (idempotencyKey && idempotencyStore.has(idempotencyKey)) {
    return res.status(202).json(idempotencyStore.get(idempotencyKey));
  }

  const applicationId = uuidv4();
  const application = {
    applicationId,
    amount,
    term,
    nationalId,
    name: name || (req.user ? req.user.name || req.user.preferred_username : 'Unknown'),
    applicantUsername: req.user ? req.user.preferred_username : null,
    status: 'PENDING',
    createdAt: new Date().toISOString()
  };

  applications.set(applicationId, application);

  // Publish event to Kafka
  try {
    await producer.send({
      topic: 'LoanApplicationCreated',
      messages: [
        { value: JSON.stringify(application) },
      ],
    });
    console.log(`Published LoanApplicationCreated for ${applicationId}`);
  } catch (error) {
    console.error('Failed to publish event', error);
    applications.delete(applicationId);
    return res.status(500).json(apiError('INTERNAL_ERROR', 'Failed to process application'));
  }

  const responseBody = {
    message: 'Loan application received',
    applicationId,
    status: 'PENDING',
    createdBy: req.user ? req.user.preferred_username : 'anonymous'
  };

  if (idempotencyKey) idempotencyStore.set(idempotencyKey, responseBody);

  res.status(202).json(responseBody);
});

// List Loan Applications with pagination (Protected by Keycloak)
app.get('/api/v1/loans', authenticateToken, (req, res) => {
  const page = Math.max(1, parseInt(req.query.page) || 1);
  const limit = Math.min(Math.max(1, parseInt(req.query.limit) || 20), 100);
  const all = Array.from(applications.values());
  const start = (page - 1) * limit;
  res.json({
    data: all.slice(start, start + limit),
    pagination: { page, limit, total: all.length, totalPages: Math.ceil(all.length / limit) }
  });
});

// Check Loan Status (Protected by Keycloak)
app.get('/api/v1/loans/:id', authenticateToken, (req, res) => {
  const application = applications.get(req.params.id);
  if (!application) {
    return res.status(404).json(apiError('NOT_FOUND', `Application ${req.params.id} not found`));
  }
  res.json(application);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Loan Origination Service running on port ${PORT}`);
});
