const express = require('express');
const morgan = require('morgan');
const axios = require('axios');
const { Kafka } = require('kafkajs');

const app = express();
app.use(morgan('dev'));

// Environment Variables
const PARTNER_URL = process.env.PARTNER_URL || 'http://localhost:3001';
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

async function processApplication(application) {
  try {
    console.log(`Requesting credit score for nationalId: ${application.nationalId}`);
    // Call Partner Mock Service
    const response = await axios.get(`${PARTNER_URL}/api/partner/credit-score`, {
      params: { nationalId: application.nationalId }
    });
    
    const { score, rating } = response.data;
    console.log(`Received score: ${score} (${rating})`);

    // Simple Decision Logic
    // E.g., Approve if score > 600, else Reject
    const decision = score > 600 ? 'APPROVED' : 'REJECTED';

    // Publish Decision
    await producer.send({
      topic: 'CreditDecisionMade',
      messages: [
        { 
          value: JSON.stringify({
            applicationId: application.applicationId,
            decision,
            score,
            rating,
            timestamp: new Date().toISOString()
          }) 
        },
      ],
    });
    console.log(`Published CreditDecisionMade for ${application.applicationId} - ${decision}`);

  } catch (error) {
    console.error(`Error processing application ${application.applicationId}`, error.message);
    // In a real system, we'd handle retries, dead letter queues, or mark it as FAILED
  }
}

// Health check endpoint
app.get('/health', (req, res) => res.send('OK'));

const PORT = process.env.PORT || 3002;
app.listen(PORT, () => {
  console.log(`Credit Decision Service running on port ${PORT}`);
});
