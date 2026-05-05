import { createFetchGateway } from "./fetchLimit";
import { fetchGlobalMax, fetchPerHostMax } from "./config";
import { loadHostCooldownFromEnv } from "./hostCooldown";
import { loadHostPacerFromEnv } from "./hostPacer";

export const fetchGateway = createFetchGateway(fetchGlobalMax, fetchPerHostMax);
export const {
  pacer: hostPacer,
  minGapMs: fetchMinGapPerHostMs,
  jitterMaxMs: fetchGapJitterMs
} = loadHostPacerFromEnv();
export const {
  cooldown: hostCooldown,
  baseBackoffMs: fetchHostCooldownBaseMs,
  maxBackoffMs: fetchHostCooldownMaxMs
} = loadHostCooldownFromEnv();
