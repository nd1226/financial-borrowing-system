const express = require('express');
const morgan = require('morgan');
const axios = require('axios');
const { Kafka } = require('kafkajs');
const promClient = require('prom-client');
promClient.collectDefaultMetrics();

const app = express();
app.use(morgan('dev'));

// Environment Variables
const PARTNER_URL = process.env.PARTNER_URL || 'http://localhost:3001';
const PARTNER_API_KEY = process.env.PARTNER_API_KEY || '';
const KAFKA_BROKERS = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:9092'];

// Kafka Configuration
const kafka = new Kafka({
  clientId: 'credit-decision-service',
  brokers: KAFKA_BROKERS
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'credit-decision-group' });

async function initKafka() {
  try {
    await producer.connect();
    console.log('Connected to Kafka Producer');
    
    await consumer.connect();
    await consumer.subscribe({ topic: 'LoanApplicationCreated', fromBeginning: true });
    
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const application = JSON.parse(message.value.toString());
        console.log(`Received LoanApplicationCreated for ${application.applicationId}`);
        await processApplication(application);
      },
    });
    console.log('Kafka Consumer listening on LoanApplicationCreated');
  } catch (error) {
    console.error('Error connecting to Kafka', error);
  }
}
initKafka();

const PARTNER_TIMEOUT_MS = 5000;
const MAX_RETRIES = 3;

async function fetchCreditScore(nationalId) {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const response = await axios.get(`${PARTNER_URL}/api/partner/credit-score`, {
        params: { nationalId },
        timeout: PARTNER_TIMEOUT_MS,
        headers: { ...(PARTNER_API_KEY && { 'X-Api-Key': PARTNER_API_KEY }) }
      });
      return response.data;
    } catch (error) {
      if (attempt === MAX_RETRIES) throw error;
      // exponential backoff: 1s, 2s, 4s
      await new Promise(resolve => setTimeout(resolve, 1000 * Math.pow(2, attempt - 1)));
      console.warn(`Retry ${attempt}/${MAX_RETRIES} for nationalId ${nationalId}: ${error.message}`);
    }
  }
}

async function publishToDLQ(application, errorMessage) {
  await producer.send({
    topic: 'LoanApplication.DLQ',
    messages: [{
      value: JSON.stringify({
        applicationId: application.applicationId,
        originalTopic: 'LoanApplicationCreated',
        error: errorMessage,
        timestamp: new Date().toISOString()
      })
    }]
  });
  console.error(`Sent applicationId ${application.applicationId} to DLQ: ${errorMessage}`);
}

async function processApplication(application) {
  try {
    console.log(`Requesting credit score for nationalId: ${application.nationalId}`);
    const { score, rating } = await fetchCreditScore(application.nationalId);
    console.log(`Received score: ${score} (${rating})`);

    const decision = score > 600 ? 'APPROVED' : 'REJECTED';

    await producer.send({
      topic: 'CreditDecisionMade',
      messages: [{
        value: JSON.stringify({
          applicationId: application.applicationId,
          decision,
          score,
          rating,
          timestamp: new Date().toISOString()
        })
      }]
    });
    console.log(`Published CreditDecisionMade for ${application.applicationId} - ${decision}`);

  } catch (error) {
    console.error(`Error processing application ${application.applicationId}`, error.message);
    await publishToDLQ(application, error.message);
  }
}

// Health check endpoint
app.get('/health', (req, res) => res.send('OK'));

app.get('/metrics', async (req, res) => {
  res.set('Content-Type', promClient.register.contentType);
  res.end(await promClient.register.metrics());
});

const PORT = process.env.PORT || 3002;
app.listen(PORT, () => {
  console.log(`Credit Decision Service running on port ${PORT}`);
});
