const { app } = require("@azure/functions");
const axios = require("axios");
const { EventHubProducerClient } = require("@azure/event-hubs");
const { DefaultAzureCredential } = require("@azure/identity");
const { SecretClient } = require("@azure/keyvault-secrets");

// Timer Trigger (every 30 seconds)
app.timer("weatherapifunction", {
    schedule: "*/30 * * * * *",
    handler: async (myTimer, context) => {
        if (myTimer.pastDue) {
            context.log("The timer is past due!");
        }

        context.log("Node.js timer trigger function executed.");

        const EVENT_HUB_NAME = "weatherstreamingeventhub";
        const EVENT_HUB_NAMESPACE = "Weather-Streaming-Namespace.servicebus.windows.net";

        const credential = new DefaultAzureCredential();

        const producer = new EventHubProducerClient(
            EVENT_HUB_NAMESPACE,
            EVENT_HUB_NAME,
            credential
        );

        // Send event
        async function sendEvent(event) {
            const batch = await producer.createBatch();
            batch.tryAdd({ body: event });
            await producer.sendBatch(batch);
        }

        // API calls
        async function getCurrentWeather(baseUrl, apiKey, location) {
            const url = `${baseUrl}/current.json`;
            const response = await axios.get(url, {
                params: { key: apiKey, q: location, aqi: "yes" }
            });
            return response.data;
        }

        async function getForecastWeather(baseUrl, apiKey, location, days) {
            const url = `${baseUrl}/forecast.json`;
            const response = await axios.get(url, {
                params: { key: apiKey, q: location, days }
            });
            return response.data;
        }

        async function getAlerts(baseUrl, apiKey, location) {
            const url = `${baseUrl}/alerts.json`;
            const response = await axios.get(url, {
                params: { key: apiKey, q: location, alerts: "yes" }
            });
            return response.data;
        }

        // Flatten data
        function flattenData(currentWeather, forecastWeather, alerts) {
            const location = currentWeather.location || {};
            const current = currentWeather.current || {};
            const condition = current.condition || {};
            const airQuality = current.air_quality || {};
            const forecast = forecastWeather.forecast?.forecastday || [];
            const alertList = alerts.alerts?.alert || [];

            return {
                name: location.name,
                region: location.region,
                country: location.country,
                lat: location.lat,
                lon: location.lon,
                localtime: location.localtime,
                temp_c: current.temp_c,
                is_day: current.is_day,
                condition_text: condition.text,
                condition_icon: condition.icon,
                wind_kph: current.wind_kph,
                wind_degree: current.wind_degree,
                wind_dir: current.wind_dir,
                pressure_mb: current.pressure_mb,
                precip_mm: current.precip_mm,
                humidity: current.humidity,
                cloud: current.cloud,
                feelslike_c: current.feelslike_c,
                uv: current.uv,

                air_quality: {
                    co: airQuality.co,
                    no2: airQuality.no2,
                    o3: airQuality.o3,
                    so2: airQuality.so2,
                    pm2_5: airQuality.pm2_5,
                    pm10: airQuality.pm10,
                    "us-epa-index": airQuality["us-epa-index"],
                    "gb-defra-index": airQuality["gb-defra-index"]
                },

                alerts: alertList.map(a => ({
                    headline: a.headline,
                    severity: a.severity,
                    description: a.description,
                    instruction: a.instruction
                })),

                forecast: forecast.map(d => ({
                    date: d.date,
                    maxtemp_c: d.day?.maxtemp_c,
                    mintemp_c: d.day?.mintemp_c,
                    condition: d.day?.condition?.text
                }))
            };
        }

        // Key Vault
        async function getSecret(vaultUrl, secretName) {
            const client = new SecretClient(vaultUrl, credential);
            const secret = await client.getSecret(secretName);
            return secret.value;
        }

        // Main
        async function fetchWeatherData() {
            const baseUrl = "https://api.weatherapi.com/v1";
            const location = "Noida";

            const VAULT_URL = "https://key-rt-weather-streaming.vault.azure.net/";
            const API_KEY_SECRET_NAME = "WeatherAPIKey";

            const apiKey = await getSecret(VAULT_URL, API_KEY_SECRET_NAME);

            const current = await getCurrentWeather(baseUrl, apiKey, location);
            const forecast = await getForecastWeather(baseUrl, apiKey, location, 3);
            const alerts = await getAlerts(baseUrl, apiKey, location);

            const mergedData = flattenData(current, forecast, alerts);

            await sendEvent(mergedData);
        }

        await fetchWeatherData();
        await producer.close();
    }
});