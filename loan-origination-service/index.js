const express = require('express');
const morgan = require('morgan');
const cors = require('cors');
const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());
app.use(morgan('dev'));
app.use(cors());

// In-memory "Database"
const applications = new Map();

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

// Create Loan Application
app.post('/api/v1/loans', async (req, res) => {
  const { amount, term, nationalId, name } = req.body;
  if (!amount || !term || !nationalId) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  const applicationId = uuidv4();
  const application = {
    applicationId,
    amount,
    term,
    nationalId,
    name,
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
    return res.status(500).json({ error: 'Failed to process application' });
  }

  res.status(202).json({
    message: 'Loan application received',
    applicationId,
    status: 'PENDING'
  });
});

// Check Loan Status
app.get('/api/v1/loans/:id', (req, res) => {
  const application = applications.get(req.params.id);
  if (!application) {
    return res.status(404).json({ error: 'Application not found' });
  }
  res.json(application);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Loan Origination Service running on port ${PORT}`);
});
