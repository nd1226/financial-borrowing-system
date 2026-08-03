param (
    [string]$Username = "john_doe",
    [string]$Password = "password123",
    [string]$KeycloakUrl = "http://localhost:8080",
    [string]$Realm = "financial-realm",
    [string]$ClientId = "loan-service"
)

Write-Host "Fetching access token from Keycloak..." -ForegroundColor Cyan
Write-Host "URL: $KeycloakUrl/realms/$Realm/protocol/openid-connect/token"
Write-Host "User: $Username"

$tokenUrl = "$KeycloakUrl/realms/$Realm/protocol/openid-connect/token"
$body = @{
    grant_type = "password"
    client_id  = $ClientId
    username   = $Username
    password   = $Password
}

try {
    $response = Invoke-RestMethod -Uri $tokenUrl -Method Post -Body $body -ContentType "application/x-www-form-urlencoded"
    $token = $response.access_token
    Write-Host "`nSuccessfully retrieved Access Token:`n" -ForegroundColor Green
    Write-Host $token -ForegroundColor Yellow
    Write-Host "`nExample API Call:" -ForegroundColor Cyan
    Write-Host "Invoke-RestMethod -Uri 'http://localhost:3000/api/v1/loans' -Method Post -Headers @{ Authorization = 'Bearer $token' } -ContentType 'application/json' -Body '{\`"amount\`": 5000, \`"term\`": 12, \`"nationalId\`": \`"123456789\`"}'"
} catch {
    Write-Host "`nFailed to retrieve access token: $_" -ForegroundColor Red
}
