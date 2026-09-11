import { NodeSDK } from "@opentelemetry/sdk-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { ATTR_SERVICE_INSTANCE_ID, ATTR_SERVICE_NAME } from "@opentelemetry/semantic-conventions";
import { config } from "./config";

let sdk: NodeSDK | undefined;

export function startTelemetry(): void {
  if (!config.otlpEndpoint) return;
  sdk = new NodeSDK({
    resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: "skyscope-ws",
      [ATTR_SERVICE_INSTANCE_ID]: config.instanceId,
    }),
    traceExporter: new OTLPTraceExporter({ url: config.otlpEndpoint }),
  });
  sdk.start();
}

export async function stopTelemetry(): Promise<void> {
  await sdk?.shutdown();
}
