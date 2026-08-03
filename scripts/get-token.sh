#!/bin/bash

# Keycloak Token Acquisition Helper Script

KEYCLOAK_URL="${KEYCLOAK_URL:-http://localhost:8080}"
REALM="${REALM:-financial-realm}"
CLIENT_ID="${CLIENT_ID:-loan-service}"
USERNAME="${1:-john_doe}"
PASSWORD="${2:-password123}"

echo "Fetching access token from Keycloak..."
echo "URL: ${KEYCLOAK_URL}/realms/${REALM}/protocol/openid-connect/token"
echo "User: ${USERNAME}"

RESPONSE=$(curl -s -X POST "${KEYCLOAK_URL}/realms/${REALM}/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "client_id=${CLIENT_ID}" \
  -d "username=${USERNAME}" \
  -d "password=${PASSWORD}")

ACCESS_TOKEN=$(echo "$RESPONSE" | grep -o '"access_token":"[^"]*' | grep -o '[^"]*$')

if [ -n "$ACCESS_TOKEN" ]; then
  echo ""
  echo "Successfully retrieved Access Token:"
  echo "$ACCESS_TOKEN"
  echo ""
  echo "Example API call with token:"
  echo "curl -X POST http://localhost:3000/api/v1/loans \\"
  echo "  -H \"Content-Type: application/json\" \\"
  echo "  -H \"Authorization: Bearer $ACCESS_TOKEN\" \\"
  echo "  -d '{\"amount\": 5000, \"term\": 12, \"nationalId\": \"123456789\", \"name\": \"John Doe\"}'"
else
  echo ""
  echo "Failed to retrieve access token. Response:"
  echo "$RESPONSE"
fi
