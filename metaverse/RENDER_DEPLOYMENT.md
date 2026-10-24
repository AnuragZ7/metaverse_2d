# Deploy Metaverse on Render

The repository-root `render.yaml` defines the frontend, HTTP API, WebSocket
service, PostgreSQL database, and Redis Key Value instance.

## Deploy with a Blueprint

1. Ensure the repository is available to your Render-connected GitHub account.
2. In the Render dashboard, create a new Blueprint from that repository.
3. Review the resources and select **Free** plans before creating them.
4. Wait for the services to finish building, then open the `metaverse-frontend`
   URL.

The Blueprint shares one generated `JWT_PASSWORD` between the HTTP API and
WebSocket services, runs the database migrations before starting the API, and
configures the frontend API and WebSocket URLs.

## Free demo limitations

This configuration is intended for a temporary demo, not production:

- Free web services spin down after 15 minutes without traffic; waking up can
  take about a minute.
- The free PostgreSQL database expires 30 days after creation. After a further
  14-day grace period, Render deletes it and its data unless it is upgraded.
- The free Redis instance is in-memory and can lose its data on restart.
- Free instance hours and bandwidth are limited by Render's current plan.

Back up any data you need and upgrade to persistent paid resources before
relying on this deployment.

## Verify the deployment

- Frontend: open the static site URL shown in Render.
- API health: open `<HTTP API URL>/api/v1/health`; it should return
  `{"status":"ok"}`.
- WebSocket: the frontend uses the WebSocket service URL configured in the
  Blueprint.

If you change the generated Render service names or domains, update
`FRONTEND_URL`, `VITE_API_URL`, and `VITE_WS_URL` in the corresponding Render
service settings and redeploy.
