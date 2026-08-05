const express = require('express');
const morgan = require('morgan');
const cors = require('cors');

const app = express();
app.use(express.json());
app.use(morgan('dev'));
app.use(cors());

const PARTNER_API_KEY = process.env.PARTNER_API_KEY;

const apiError = (code, message) => ({ code, message });

// Validate internal API key when PARTNER_API_KEY is configured
function requireApiKey(req, res, next) {
  if (!PARTNER_API_KEY) return next();
  if (req.headers['x-api-key'] !== PARTNER_API_KEY) {
    return res.status(401).json(apiError('UNAUTHORIZED', 'Invalid or missing X-Api-Key'));
  }
  next();
}

// Mock Credit Bureau API
app.get('/api/partner/credit-score', requireApiKey, (req, res) => {
    const { nationalId } = req.query;

    if (!nationalId) {
        return res.status(400).json(apiError('VALIDATION_ERROR', 'nationalId is required'));
    }

    // Deterministic mock generation based on nationalId length/value for testing
    let score = 700; // Default
    if (nationalId.length > 0) {
        // Just a simple hash-like logic to return consistent score for same ID
        let hash = 0;
        for (let i = 0; i < nationalId.length; i++) {
            hash = nationalId.charCodeAt(i) + ((hash << 5) - hash);
        }
        score = 300 + Math.abs(hash) % 550; // Random score between 300 and 850
    }

    let rating = 'POOR';
    if (score >= 750) rating = 'EXCELLENT';
    else if (score >= 650) rating = 'GOOD';
    else if (score >= 550) rating = 'FAIR';

    // Simulate network delay
    setTimeout(() => {
        res.status(200).json({
            nationalId,
            score,
            rating
        });
    }, 500); // 500ms delay
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`Partner Mock Service running on port ${PORT}`);
});
