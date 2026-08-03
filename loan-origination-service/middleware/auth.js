const jwt = require('jsonwebtoken');
const jwksClient = require('jwks-rsa');

const KEYCLOAK_URL = process.env.KEYCLOAK_URL || 'http://localhost:8080';
const KEYCLOAK_REALM = process.env.KEYCLOAK_REALM || 'financial-realm';
const KEYCLOAK_ENABLED = process.env.KEYCLOAK_ENABLED !== 'false';

// Initialize JWKS Client to fetch Keycloak public certificates
const client = jwksClient({
  jwksUri: `${KEYCLOAK_URL}/realms/${KEYCLOAK_REALM}/protocol/openid-connect/certs`,
  cache: true,
  rateLimit: true,
  jwksRequestsPerMinute: 10
});

function getKey(header, callback) {
  client.getSigningKey(header.kid, (err, key) => {
    if (err) {
      return callback(err, null);
    }
    const signingKey = key.getPublicKey();
    callback(null, signingKey);
  });
}

function authenticateToken(req, res, next) {
  if (!KEYCLOAK_ENABLED) {
    req.user = { preferred_username: 'dev_user', realm_access: { roles: ['loan-applicant'] } };
    return next();
  }

  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.startsWith('Bearer ') ? authHeader.split(' ')[1] : null;

  if (!token) {
    return res.status(401).json({
      error: 'Unauthorized',
      message: 'Access token is missing or malformed. Expected Bearer token in Authorization header.'
    });
  }

  jwt.verify(token, getKey, {
    issuer: `${KEYCLOAK_URL}/realms/${KEYCLOAK_REALM}`,
    algorithms: ['RS256']
  }, (err, decoded) => {
    if (err) {
      console.error('JWT Verification Error:', err.message);
      return res.status(401).json({
        error: 'Unauthorized',
        message: 'Invalid or expired token.',
        details: err.message
      });
    }

    req.user = decoded;
    next();
  });
}

module.exports = { authenticateToken };
